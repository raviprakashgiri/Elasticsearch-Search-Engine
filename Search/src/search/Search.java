package search;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;

import org.apache.log4j.BasicConfigurator;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.SearchHit;
import org.jsoup.Jsoup;

public class Search {
	
	public static String stringJoin(String del, String[] args) {
		if (args.length == 0)
			return "";
		StringBuilder builder = new StringBuilder(args[0]);
		for (int i = 1; i < args.length; ++i)
			builder.append(" " + args[i]);
		return builder.toString();
	}

	/**
	 * Search for a given query.
	 */
	public static void main(String[] args) {
		String query = stringJoin(" ", args);
		if (!query.isEmpty())
			query = Jsoup.parse(query).text().replace("\"", "\\\"");
		if (query.isEmpty()) {
			System.out.println("Please enter a valid search query.");
			System.exit(-1);
		}

		BasicConfigurator.configure();
		TransportClient transportClient = new TransportClient();
		Client client = transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

		try {
			String preSource = String.format(Indexer.readFile("src/search/presearch.json", Charset.defaultCharset()), query);
			SearchResponse preResp = client.prepareSearch("apps").setTypes("app").setSize(1)
					.setQuery(preSource).execute().actionGet();
			double maxScore = preResp.getHits().getMaxScore();

			String searchSource = String.format(Indexer.readFile("src/search/search.json", Charset.defaultCharset()), query, maxScore);
			SearchResponse resp = client.prepareSearch("apps").setTypes("app").setSize(200).setQuery(searchSource).execute().actionGet();

			System.out.println("\nTOP " + (resp.getHits().getTotalHits()) + " RESULTS:\n\n");

			int res = 1;
			for (SearchHit hit : resp.getHits()) {
				Map<String, Object> map = hit.getSource();
				String title = (String) map.get("title");
				String category = (String) map.get("category");
				String[] domains = new String[5];
				domains[0] = (String) map.get("domain1");
				domains[1] = (String) map.get("domain2");
				domains[2] = (String) map.get("domain3");
				domains[3] = (String) map.get("domain4");
				domains[4] = (String) map.get("domain5");
				String publisher = (String) map.get("publisher");
				String description = (String) map.get("description");

				StringBuilder builder = new StringBuilder(domains[0]);
				for (int i = 1; i < domains.length; ++i) {
					if (domains[i] != null)
						builder.append(", " + domains[1]);
					else
						break;
				}
				String domString = builder.toString();

				System.out.println("Result " + res + ":");
				System.out.println("TITLE:     " + title);
				System.out.println("PUBLISHER: " + publisher);
				System.out.println("DOMAINS:   " + domString);
				System.out.println("CATEGORY:  " + category);
				System.out.println("DESCRIPTION:");
				System.out.println("   " + description + "\n\n");
				++res;
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		transportClient.close();
	}

}
