/*
 * Copyright 2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.convert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.SpELContext;
import org.springframework.data.mongodb.core.convert.ReferenceResolver.ReferenceContext;
import org.springframework.data.mongodb.core.mapping.ManualReference;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.util.BsonUtils;
import org.springframework.data.mongodb.util.json.ParameterBindingContext;
import org.springframework.data.mongodb.util.json.ParameterBindingDocumentCodec;
import org.springframework.data.mongodb.util.json.ValueProvider;
import org.springframework.data.util.Lazy;
import org.springframework.data.util.Streamable;
import org.springframework.expression.EvaluationContext;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import com.mongodb.DBRef;

/**
 * @author Christoph Strobl
 */
public class ReferenceReader {

	private final ParameterBindingDocumentCodec codec;

	private final Lazy<MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty>> mappingContext;
	private final BiFunction<MongoPersistentProperty, Document, Object> documentConversionFunction;
	private final Supplier<SpELContext> spelContextSupplier;

	public ReferenceReader(MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext,
			BiFunction<MongoPersistentProperty, Document, Object> documentConversionFunction,
			Supplier<SpELContext> spelContextSupplier) {

		this(() -> mappingContext, documentConversionFunction, spelContextSupplier);
	}

	public ReferenceReader(
			Supplier<MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty>> mappingContextSupplier,
			BiFunction<MongoPersistentProperty, Document, Object> documentConversionFunction,
			Supplier<SpELContext> spelContextSupplier) {

		this.mappingContext = Lazy.of(mappingContextSupplier);
		this.documentConversionFunction = documentConversionFunction;
		this.spelContextSupplier = spelContextSupplier;
		this.codec = new ParameterBindingDocumentCodec();
	}

	Object readReference(MongoPersistentProperty property, Object value,
			BiFunction<ReferenceContext, Bson, Stream<Document>> lookupFunction) {

		SpELContext spELContext = spelContextSupplier.get();

		Document filter = computeFilter(property, value, spELContext);
		ReferenceContext referenceContext = computeReferenceContext(property, value, spELContext);

		Stream<Document> result = lookupFunction.apply(referenceContext, filter);

		if (property.isCollectionLike()) {

			if (filter.containsKey("$or")) {
				List<Document> ors = filter.get("$or", List.class);
				result = result.sorted((o1, o2) -> compareAgainstReferenceIndex(ors, o1, o2));
			}

			return result.map(it -> documentConversionFunction.apply(property, it)).collect(Collectors.toList());
		}

		return result.map(it -> documentConversionFunction.apply(property, it)).findFirst().orElse(null);
	}

	private ReferenceContext computeReferenceContext(MongoPersistentProperty property, Object value,
			SpELContext spELContext) {

		if (value instanceof Iterable) {
			value = ((Iterable<?>) value).iterator().next();
		}

		if (value instanceof Document) {

			Document ref = (Document) value;
			if (property.isAnnotationPresent(ManualReference.class)) {

				String collection = property.getRequiredAnnotation(ManualReference.class).collection();
				if (StringUtils.hasText(collection)) {

					Object coll = collection;
					ParameterBindingContext bindingContext = bindingContext(property, value, spELContext);

					if (!BsonUtils.isJsonDocument(collection) && collection.contains("?#{")) {
						String s = "{ 'target-collection' : " + collection + "}";
						coll = new ParameterBindingDocumentCodec().decode(s, bindingContext).getString("target-collection");
					} else {
						coll = bindingContext.evaluateExpression(collection);
					}

					if (coll != null) {
						return new ReferenceContext(ref.getString("db"), ObjectUtils.nullSafeToString(coll));
					}
				}
			}

			return new ReferenceContext(ref.getString("db"), ref.get("collection",
					mappingContext.get().getPersistentEntity(property.getAssociationTargetType()).getCollection()));
		}

		if (value instanceof DBRef) {
			return ReferenceContext.fromDBRef((DBRef) value);
		}

		return new ReferenceContext(null,
				mappingContext.get().getPersistentEntity(property.getAssociationTargetType()).getCollection());
	}

	ParameterBindingContext bindingContext(MongoPersistentProperty property, Object source, SpELContext spELContext) {

		return new ParameterBindingContext(valueProviderFor(source), spELContext.getParser(),
				() -> evaluationContextFor(property, source, spELContext));
	}

	ValueProvider valueProviderFor(Object source) {
		return (index) -> {

			if (source instanceof Document) {
				return Streamable.of(((Document) source).values()).toList().get(index);
			}
			return source;
		};
	}

	EvaluationContext evaluationContextFor(MongoPersistentProperty property, Object source, SpELContext spELContext) {

		EvaluationContext ctx = spELContext.getEvaluationContext(source);
		ctx.setVariable("target", source);
		ctx.setVariable(property.getName(), source);

		return ctx;
	}

	Document computeFilter(MongoPersistentProperty property, Object value, SpELContext spELContext) {

		String lookup = property.getRequiredAnnotation(ManualReference.class).lookup();

		if (property.isCollectionLike() && value instanceof Collection) {

			List<Document> ors = new ArrayList<>();
			for (Object entry : (Collection) value) {

				Document decoded = codec.decode(lookup, bindingContext(property, entry, spELContext));
				ors.add(decoded);
			}

			return new Document("$or", ors);
		}

		return codec.decode(lookup, bindingContext(property, value, spELContext));
	}

	int compareAgainstReferenceIndex(List<Document> referenceList, Document document1, Document document2) {

		for (int i = 0; i < referenceList.size(); i++) {

			Set<Entry<String, Object>> entries = referenceList.get(i).entrySet();
			if (document1.entrySet().containsAll(entries)) {
				return -1;
			}
			if (document2.entrySet().containsAll(entries)) {
				return 1;
			}
		}
		return referenceList.size();
	}

}
