import static org.assertj.core.api.Assertions.*;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.aggregation.AggregationExpression;
import org.springframework.data.mongodb.core.aggregation.ComparisonOperators;

/**
 * Unit tests for (@link ComparisonOperators}
 */
public class ComparisonOperatorsUnitTests {

	private static final AggregationExpression EXPRESSION = context -> Document.parse("{ \"$exp\" : 2 }");

	@Test
	public void rendersIsNullWithFieldReference() {

		assertThat(ComparisonOperators.valueOf("field").isNull().toDocument())
				.isEqualTo(Document.parse("{ \"$eq\" : [ \"$field\", null ]}"));
	}

	@Test
	public void rendersIsNullWithExpression() {

		assertThat(ComparisonOperators.valueOf(EXPRESSION).isNull().toDocument())
				.isEqualTo(Document.parse("{ \"$eq\" : [ { \"$exp\" : 2 } , null ]}"));
	}

	@Test
	public void rendersCmpFieldReferenceWithFieldReference() {

		assertThat(ComparisonOperators.valueOf("field1").compareTo("field2").toDocument())
				.isEqualTo(Document.parse("{ $cmp: [ \"$field1\", \"$field2\" ] }"));
	}

	@Test
	public void rendersCmpFieldReferenceWithExpression() {

		assertThat(ComparisonOperators.valueOf("field1").compareTo(EXPRESSION).toDocument())
				.isEqualTo(Document.parse("{ $cmp: [ \"$field1\", { \"$exp\" : 2 } ] }"));
	}

	@Test
	public void rendersCmpFieldReferenceWithValue() {
		
		assertThat(ComparisonOperators.valueOf("field1").compareToValue(200).toDocument())
				.isEqualTo(Document.parse("{ $cmp: [ \"$field1\", 200 ] }"));
	}
	
	@Test
	public void rendersEqFieldReferenceWithFieldReference() {

		assertThat(ComparisonOperators.valueOf("field1").equalTo("field2").toDocument())
				.isEqualTo(Document.parse("{ $eq: [ \"$field1\", \"$field2\" ] }"));
	}

	@Test
	public void rendersEqFieldReferenceWithExpression() {

		assertThat(ComparisonOperators.valueOf("field1").equalTo(EXPRESSION).toDocument())
				.isEqualTo(Document.parse("{ $eq: [ \"$field1\", { \"$exp\" : 2 } ] }"));
	}

	@Test
	public void rendersEqFieldReferenceWithValue() {
		
		assertThat(ComparisonOperators.valueOf("field1").equalToValue(200).toDocument())
				.isEqualTo(Document.parse("{ $eq: [ \"$field1\", 200 ] }"));
	}
	
	@Test
	public void rendersGtFieldReferenceWithFieldReference() {

		assertThat(ComparisonOperators.valueOf("field1").greaterThan("field2").toDocument())
				.isEqualTo(Document.parse("{ $gt: [ \"$field1\", \"$field2\" ] }"));
	}

	@Test
	public void rendersGtFieldReferenceWithExpression() {

		assertThat(ComparisonOperators.valueOf("field1").greaterThan(EXPRESSION).toDocument())
				.isEqualTo(Document.parse("{ $gt: [ \"$field1\", { \"$exp\" : 2 } ] }"));
	}

	@Test
	public void rendersGtFieldReferenceWithValue() {
		
		assertThat(ComparisonOperators.valueOf("field1").greaterThanValue(200).toDocument())
				.isEqualTo(Document.parse("{ $gt: [ \"$field1\", 200 ] }"));
	}
	
	@Test
	public void rendersGteFieldReferenceWithFieldReference() {

		assertThat(ComparisonOperators.valueOf("field1").greaterThanEqualTo("field2").toDocument())
				.isEqualTo(Document.parse("{ $gte: [ \"$field1\", \"$field2\" ] }"));
	}

	@Test
	public void rendersGteFieldReferenceWithExpression() {

		assertThat(ComparisonOperators.valueOf("field1").greaterThanEqualTo(EXPRESSION).toDocument())
				.isEqualTo(Document.parse("{ $gte: [ \"$field1\", { \"$exp\" : 2 } ] }"));
	}

	@Test
	public void rendersGteFieldReferenceWithValue() {
		
		assertThat(ComparisonOperators.valueOf("field1").greaterThanEqualToValue(200).toDocument())
				.isEqualTo(Document.parse("{ $gte: [ \"$field1\", 200 ] }"));
	}
	
	@Test
	public void rendersLtFieldReferenceWithFieldReference() {

		assertThat(ComparisonOperators.valueOf("field1").lessThan("field2").toDocument())
				.isEqualTo(Document.parse("{ $lt: [ \"$field1\", \"$field2\" ] }"));
	}

	@Test
	public void rendersLtFieldReferenceWithExpression() {

		assertThat(ComparisonOperators.valueOf("field1").lessThan(EXPRESSION).toDocument())
				.isEqualTo(Document.parse("{ $lt: [ \"$field1\", { \"$exp\" : 2 } ] }"));
	}

	@Test
	public void rendersLtFieldReferenceWithValue() {
		
		assertThat(ComparisonOperators.valueOf("field1").lessThanValue(200).toDocument())
				.isEqualTo(Document.parse("{ $lt: [ \"$field1\", 200 ] }"));
	}
	
	@Test
	public void rendersLteFieldReferenceWithFieldReference() {

		assertThat(ComparisonOperators.valueOf("field1").lessThanEqualTo("field2").toDocument())
				.isEqualTo(Document.parse("{ $lte: [ \"$field1\", \"$field2\" ] }"));
	}

	@Test
	public void rendersLteFieldReferenceWithExpression() {

		assertThat(ComparisonOperators.valueOf("field1").lessThanEqualTo(EXPRESSION).toDocument())
				.isEqualTo(Document.parse("{ $lte: [ \"$field1\", { \"$exp\" : 2 } ] }"));
	}

	@Test
	public void rendersLteFieldReferenceWithValue() {
		
		assertThat(ComparisonOperators.valueOf("field1").lessThanEqualToValue(200).toDocument())
				.isEqualTo(Document.parse("{ $lte: [ \"$field1\", 200 ] }"));
	}
	
	@Test
	public void rendersNeFieldReferenceWithFieldReference() {

		assertThat(ComparisonOperators.valueOf("field1").notEqualTo("field2").toDocument())
				.isEqualTo(Document.parse("{ $ne: [ \"$field1\", \"$field2\" ] }"));
	}

	@Test
	public void rendersNeFieldReferenceWithExpression() {

		assertThat(ComparisonOperators.valueOf("field1").notEqualTo(EXPRESSION).toDocument())
				.isEqualTo(Document.parse("{ $ne: [ \"$field1\", { \"$exp\" : 2 } ] }"));
	}

	@Test
	public void rendersNeFieldReferenceWithValue() {
		
		assertThat(ComparisonOperators.valueOf("field1").notEqualToValue(200).toDocument())
				.isEqualTo(Document.parse("{ $ne: [ \"$field1\", 200 ] }"));
	}
	

}
