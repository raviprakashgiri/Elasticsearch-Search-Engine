package search;

import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.BasicConfigurator;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.jsoup.Jsoup;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import scoring.AppScore;

public class Indexer {

	/**
	 * Driver program.
	 */
	public static void main(String[] args) {
		if (args.length == 0) {
			System.out.println("Syntax: [list of csv's to index]");
			System.exit(-1);
		}

		BasicConfigurator.configure();
		TransportClient transportClient = new TransportClient();
		Client client = transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

		// recreate the index
		IndicesExistsResponse resp = client.admin().indices().exists(new IndicesExistsRequest("apps")).actionGet();
		if (resp.isExists()) {
			System.out.println("Deleting the 'apps' index.");
			client.admin().indices().delete(new DeleteIndexRequest("apps")).actionGet();
		} else {
			System.out.println("The 'apps' index has not been created.");
		}

		CsvListReader reader = null;
		BulkProcessor bulkProcessor = null;

		try {
			System.out.println("Creating the 'apps' index.");

			String setting = readFile("src/search/settings.json", Charset.defaultCharset());
			client.admin().indices().create(new CreateIndexRequest("apps").source(setting)).actionGet();
			System.out.println("Created the 'apps' index.");

			bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
				public void beforeBulk(long executionId, BulkRequest request) {
					System.out.println("Going to execute new bulk composed of " + request.numberOfActions() + " actions.");
				}

				public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
					System.out.println("Executed bulk composed of " + request.numberOfActions() + " actions.");
				}

				public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
					System.err.println("Error executing bulk:");
					failure.printStackTrace();
				}

			}).setBulkActions(500).setConcurrentRequests(0).build();

			SimpleDateFormat inFormatter = new SimpleDateFormat("MMM dd, yyyy");
			SimpleDateFormat outFormatter = new SimpleDateFormat("yyyy-MM-dd");

			for (String fname : args) {
				try {
					reader = new CsvListReader(new FileReader(fname), CsvPreference.STANDARD_PREFERENCE);

					List<String> headercols = new ArrayList<String>(reader.read());
					int numRatingIndex = headercols.indexOf("Num Ratings");
					int ratingIndex = headercols.indexOf("Rating");
					int installsIndex = headercols.indexOf("Installs");
					int updatedIndex = headercols.indexOf("Updated");
					int sizeIndex = headercols.indexOf("Size");
					int descIndex = headercols.indexOf("Description");

					if (numRatingIndex == -1 || ratingIndex == -1 || installsIndex == -1 || sizeIndex == -1 || descIndex == -1) {
						System.err.println("One of the indices was not found.");
						continue;
					}

					headercols.add("AppScore");
					String[] header = headercols.toArray(new String[headercols.size()]);
					for (int i = 0; i < header.length; ++i)
						header[i] = header[i].toLowerCase();

					List<String> rawrow;
					ArrayList<String> row;
					while ((rawrow = reader.read()) != null) {
						row = new ArrayList<String>(rawrow);
						AppScore.processRow(row, numRatingIndex, ratingIndex, installsIndex, sizeIndex);
						row.set(descIndex, Jsoup.parse(row.get(descIndex)).text());

						try {
							Date date = inFormatter.parse(row.get(updatedIndex).trim());
							row.set(updatedIndex, outFormatter.format(date));
						} catch (ParseException e) {
							row.set(updatedIndex, outFormatter.format(DateTime.now().minusMonths(5).toDate()));
						}

						Map<String, Object> json = new HashMap<String, Object>();
						int i = 0;
						String id = null;
						for (String item : row) {
							if (i > 0)
								json.put(header[i], item);
							else
								id = item;
							++i;
						}
						bulkProcessor.add(new IndexRequest("apps", "app", id).source(json));
					}
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					try {
						reader.close();
					} catch (IOException e) {
					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			bulkProcessor.close();
			transportClient.close();
		}
	}

	static String readFile(String path, Charset encoding) throws IOException 
	{
		byte[] encoded = Files.readAllBytes(Paths.get(path));
		return new String(encoded, encoding);
	}
}
