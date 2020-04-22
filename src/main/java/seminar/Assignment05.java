package seminar;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nonnull;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

public class Assignment05 {

	private final static Logger logger = Logger.getLogger(Assignment05.class.getName());

	public static void main(final String[] args) {
		final AmazonDynamoDB dbClient = createDynamoDbClient();

		createMoviesTable(dbClient);
	}

	@Nonnull
	private static AmazonDynamoDB createDynamoDbClient() {
		return AmazonDynamoDBClientBuilder.standard()
				.withRegion(Regions.US_EAST_1)
				.build();
	}

	private static void createMoviesTable(@Nonnull final AmazonDynamoDB dbClient) {
		final CreateTableRequest request = new CreateTableRequest()
				.withTableName("Movies")
				.withAttributeDefinitions(
						new AttributeDefinition("year", ScalarAttributeType.N),
						new AttributeDefinition("title", ScalarAttributeType.S)
				)
				.withKeySchema(
						new KeySchemaElement("year", KeyType.HASH),
						new KeySchemaElement("title", KeyType.RANGE)
				)
				.withProvisionedThroughput(
						new ProvisionedThroughput()
								.withReadCapacityUnits(10L)
								.withWriteCapacityUnits(10L)
				);

		dbClient.createTable(request);

		logger.log(Level.INFO, "Table {0} created", request.getTableName());
	}
}
