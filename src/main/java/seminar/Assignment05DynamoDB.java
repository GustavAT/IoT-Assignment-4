package seminar;

import java.io.FileReader;
import java.io.Reader;
import java.net.URL;
import java.util.List;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.google.common.collect.ImmutableMap;

public class Assignment05DynamoDB {

	private static final Logger logger = Logger.getLogger(Assignment05DynamoDB.class.getName());

	private static final Regions REGION = Regions.US_EAST_1;
	private static final String TABLE_NAME = "Movies";

	private static final String MOVIES_FILE_NAME = "moviedata.json";
	private static final int MOVIES_LIMIT = 100;

	public static void main(final String[] args) {
		final DynamoDB dbClient = createDynamoDbClient();

		listTables(dbClient);

		final Table moviesTable = createMoviesTable(dbClient);
		printTableInfo(moviesTable);

		listTables(dbClient);

		final JSONArray movies = loadMoviesSampleData();

		if (movies == null) {
			logger.log(Level.INFO, "Skip inserting sample data");
		} else {
			insertMoviesDataInTable(moviesTable, movies);
			printTableInfo(moviesTable);

			deleteMovieFromTable(moviesTable);
			printTableInfo(moviesTable);

			queryMoviesTable(moviesTable);
		}

		logger.log(Level.INFO, "Press ENTER to delete the table");
		new Scanner(System.in).nextLine();

		deleteTable(moviesTable);
	}

	@Nonnull
	private static DynamoDB createDynamoDbClient() {
		final AmazonDynamoDB amazonDynamoDB = AmazonDynamoDBClientBuilder.standard()
				.withRegion(REGION)
				.build();

		return new DynamoDB(amazonDynamoDB);
	}

	/**
	 * Creates a Movies table with title and year attributes.
	 * @param dynamoDB DynamoDB client
	 * @return Movies table
	 */
	private static Table createMoviesTable(@Nonnull final DynamoDB dynamoDB) {
		logger.log(Level.INFO, "Create movies table");

		final CreateTableRequest request = new CreateTableRequest()
				.withTableName(TABLE_NAME)
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

		final Table table = dynamoDB.createTable(request);
		final TableDescription description = table.getDescription();

		try {
			table.waitForActive();
		} catch (final Exception e) {
			logger.log(Level.WARNING, "Error waiting for table to be active", e.fillInStackTrace());
		}

		logger.log(Level.INFO, "Table {0} with id {1} created",
				new Object[] {description.getTableName(), description.getTableId()});

		return table;
	}

	/**
	 * Print information about given {@code table}.
	 * @param table A table
	 */
	private static void printTableInfo(@Nonnull final Table table) {
		final TableDescription description = table.describe();

		logger.log(Level.INFO, "Info for {0}: Id: {1}, Status: {2}, Item-count: {3}", new Object[]{
				description.getTableName(),
				description.getTableId(),
				description.getTableStatus(),
				description.getItemCount()
		});
	}

	/**
	 * List all tables in the {@code US_EAST_1} region.
	 * @param dbClient Amazon DynamoDB client
	 */
	private static void listTables(@Nonnull final DynamoDB dbClient) {
		logger.log(Level.INFO, "Listing all tables in {0} region", REGION);

		final TableCollection<ListTablesResult> tables = dbClient.listTables();

		for (final Table table : tables) {
			logger.log(Level.INFO, table.getTableName());
		}
	}

	/**
	 * Loads Movies sample data
	 * @return Sample movies
	 */
	@CheckForNull
	private static JSONArray loadMoviesSampleData() {
		final URL moviesUrl = Assignment05DynamoDB.class.getClassLoader().getResource(MOVIES_FILE_NAME);

		if (moviesUrl == null) {
			return null;
		}

		final String moviesPath = moviesUrl.getPath();
		final JSONParser parser = new JSONParser();

		try (final Reader reader = new FileReader(moviesPath)) {
			return (JSONArray) parser.parse(reader);
		} catch (final Exception e) {
			logger.log(Level.SEVERE, "Could not parse JSON from file {0}", moviesPath);
		}

		return null;
	}

	/**
	 * Insert given {@code movies} data limited by {@code MOVIES_LIMIT} to given {@code table}.
	 * @param table A table
	 * @param jsonMovies Movies json data
	 */
	private static void insertMoviesDataInTable(@Nonnull final Table table, @Nonnull final JSONArray jsonMovies) {
		final List<JSONObject> movies = Stream.of(jsonMovies.toArray())
				.limit(MOVIES_LIMIT)
				.filter(movie -> movie instanceof JSONObject)
				.map(movie -> (JSONObject) movie)
				.collect(Collectors.toList());

		for (final JSONObject movie : movies) {
			final String title = (String) movie.get("title");
			final long year = (long) movie.get("year");
			final String info = movie.get("info").toString();

			final Item item = new Item()
					.withPrimaryKey("year", year, "title", title)
					.withJSON("info", info);

			table.putItem(item);
			logger.log(Level.INFO, "Put item {0} to table", item);
		}
	}

	/**
	 * Delete a movie from given {@code table}.
	 * @param table A Table
	 */
	private static void deleteMovieFromTable(@Nonnull final Table table) {
		table.deleteItem("year", 2013L, "title", "12 Years a Slave");
		logger.log(Level.INFO, "Deleted item from table");
	}

	/**
	 * Query given {@code table}
	 * @param table A table
	 */
	private static void queryMoviesTable(@Nonnull final Table table) {
		logger.log(Level.INFO, "Query movies table for movies in year 2013");

		final QuerySpec querySpec = new QuerySpec()
				.withKeyConditionExpression("#yr = :yyyy")
				.withNameMap(ImmutableMap.of("#yr", "year"))
				.withValueMap(ImmutableMap.of(":yyyy", 2013L));
		final ItemCollection<QueryOutcome> items = table.query(querySpec);

		for (final Item item : items) {
			logger.log(Level.INFO, "Item: {0}", item);
		}
	}


	/**
	 * Delete the given {@code table}
	 * @param table A table
	 */
	private static void deleteTable(@Nonnull final Table table) {
		logger.log(Level.INFO, "Delete movies table");

		final DeleteTableResult result = table.delete();
		final TableDescription description = result.getTableDescription();

		try {
			table.waitForDelete();
		} catch (final InterruptedException e) {
			logger.log(Level.WARNING, "Error waiting for table to be active", e.fillInStackTrace());
		}

		logger.log(Level.INFO, "Deleted table {0} with id {1}", new Object[]{description.getTableName(), description.getTableId()});
	}
}
