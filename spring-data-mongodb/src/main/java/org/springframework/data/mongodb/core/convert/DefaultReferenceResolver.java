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

import static org.springframework.data.mongodb.core.convert.ReferenceLookupDelegate.*;

import java.util.Collections;

import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.DocumentReference;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.util.Assert;

/**
 * {@link ReferenceResolver} implementation that uses a given {@link ReferenceLookupDelegate} to load and convert entity
 * associations expressed via a {@link MongoPersistentProperty persitent property}. Creates {@link LazyLoadingProxy
 * proxies} for associations that should be lazily loaded.
 *
 * @author Christoph Strobl
 */
public class DefaultReferenceResolver implements ReferenceResolver {

	private final ReferenceLoader referenceLoader;

	private final LookupFunction collectionLookupFunction = (filter, ctx) -> getReferenceLoader().fetchMany(filter, ctx);
	private final LookupFunction singleValueLookupFunction = (filter, ctx) -> {
		Object target = getReferenceLoader().fetchOne(filter, ctx);
		return target == null ? Collections.emptyList() : Collections.singleton(getReferenceLoader().fetchOne(filter, ctx));
	};

	/**
	 * Create a new instance of {@link DefaultReferenceResolver}.
	 * 
	 * @param referenceLoader must not be {@literal null}.
	 */
	public DefaultReferenceResolver(ReferenceLoader referenceLoader) {
		
		Assert.notNull(referenceLoader, "ReferenceLoader must not be null!");
		this.referenceLoader = referenceLoader;
	}

	@Override
	public Object resolveReference(MongoPersistentProperty property, Object source,
			ReferenceLookupDelegate referenceLookupDelegate, MongoEntityReader entityReader) {

		LookupFunction lookupFunction = (property.isCollectionLike() || property.isMap()) ? collectionLookupFunction
				: singleValueLookupFunction;

		if (isLazyReference(property)) {
			return createLazyLoadingProxy(property, source, referenceLookupDelegate, lookupFunction, entityReader);
		}

		return referenceLookupDelegate.readReference(property, source, lookupFunction, entityReader);
	}

	/**
	 * Check if the association expressed by the given {@link MongoPersistentProperty property} should be resolved lazily.
	 *
	 * @param property
	 * @return return {@literal true} if the defined association is lazy.
	 * @see DBRef#lazy()
	 * @see DocumentReference#lazy()
	 */
	protected boolean isLazyReference(MongoPersistentProperty property) {

		if (property.isDocumentReference()) {
			return property.getDocumentReference().lazy();
		}

		return property.getDBRef() != null && property.getDBRef().lazy();
	}

	/**
	 * The {@link ReferenceLoader} executing the lookup.
	 *
	 * @return never {@literal null}.
	 */
	protected ReferenceLoader getReferenceLoader() {
		return referenceLoader;
	}

	private Object createLazyLoadingProxy(MongoPersistentProperty property, Object source,
			ReferenceLookupDelegate referenceLookupDelegate, LookupFunction lookupFunction, MongoEntityReader entityReader) {
		return new LazyLoadingProxyFactory(referenceLookupDelegate).createLazyLoadingProxy(property, source, lookupFunction,
				entityReader);
	}
}
