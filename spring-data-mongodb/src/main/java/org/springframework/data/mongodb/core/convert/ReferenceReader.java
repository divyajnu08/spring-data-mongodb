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
import org.springframework.data.mongodb.core.mapping.DocumentReference;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.util.BsonUtils;
import org.springframework.data.mongodb.util.json.ParameterBindingContext;
import org.springframework.data.mongodb.util.json.ParameterBindingDocumentCodec;
import org.springframework.data.mongodb.util.json.ValueProvider;
import org.springframework.data.util.Lazy;
import org.springframework.data.util.Streamable;
import org.springframework.expression.EvaluationContext;
import org.springframework.lang.Nullable;
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

		if (value instanceof DBRef) {
			return ReferenceContext.fromDBRef((DBRef) value);
		}

		if (value instanceof Document) {

			Document ref = (Document) value;

			if (property.isAnnotationPresent(DocumentReference.class)) {

				ParameterBindingContext bindingContext = bindingContext(property, value, spELContext);
				DocumentReference documentReference = property.getRequiredAnnotation(DocumentReference.class);

				String targetDatabase = parseValueOrGet(documentReference.db(), bindingContext,
						() -> ref.get("db", String.class));
				String targetCollection = parseValueOrGet(documentReference.collection(), bindingContext,
						() -> ref.get("collection",
								mappingContext.get().getPersistentEntity(property.getAssociationTargetType()).getCollection()));
				Document sort = parseValueOrGet(documentReference.sort(), bindingContext, () -> null);
				return new ReferenceContext(targetDatabase, targetCollection, sort);
			}

			return new ReferenceContext(ref.getString("db"), ref.get("collection",
					mappingContext.get().getPersistentEntity(property.getAssociationTargetType()).getCollection()), null);
		}

		if (property.isAnnotationPresent(DocumentReference.class)) {

			ParameterBindingContext bindingContext = bindingContext(property, value, spELContext);
			DocumentReference documentReference = property.getRequiredAnnotation(DocumentReference.class);

			String targetDatabase = parseValueOrGet(documentReference.db(), bindingContext, () -> null);
			String targetCollection = parseValueOrGet(documentReference.collection(), bindingContext,
					() -> mappingContext.get().getPersistentEntity(property.getAssociationTargetType()).getCollection());
			Document sort = parseValueOrGet(documentReference.sort(), bindingContext, () -> null);

			return new ReferenceContext(targetDatabase, targetCollection, sort);
		}

		return new ReferenceContext(null,
				mappingContext.get().getPersistentEntity(property.getAssociationTargetType()).getCollection(), null);
	}

	@Nullable
	private <T> T parseValueOrGet(String value, ParameterBindingContext bindingContext, Supplier<T> defaultValue) {

		if (!StringUtils.hasText(value)) {
			return defaultValue.get();
		}

		if (!BsonUtils.isJsonDocument(value) && value.contains("?#{")) {
			String s = "{ 'target-value' : " + value + "}";
			T evaluated = (T) new ParameterBindingDocumentCodec().decode(s, bindingContext).get("target-value ");
			return evaluated != null ? evaluated : defaultValue.get();
		}

		T evaluated = (T) bindingContext.evaluateExpression(value);
		return evaluated != null ? evaluated : defaultValue.get();
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

		String lookup = property.getRequiredAnnotation(DocumentReference.class).lookup();

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
