package result;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.BasicConfigurator;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.SearchHit;
import org.jsoup.Jsoup;

/**
 * Servlet implementation class Results
 */
@WebServlet("/Results")
public class Results extends HttpServlet {
	private static final long serialVersionUID = 1L;
	
    /**
     * @throws IOException 
     * @see HttpServlet#HttpServlet()
     */
    public Results() {
        super();
		BasicConfigurator.configure();
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		PrintWriter out = response.getWriter();
		response.setContentType("text/html");
		String query = request.getParameter("q");
		if (!query.isEmpty())
			query = Jsoup.parse(query).text().replace("\"", "\\\"");
		if (query.isEmpty()) {
			out.println("<h2>Please enter a valid search query.</2>");
			return;
		}

		TransportClient transportClient = new TransportClient();
		Client client = transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

		String sPreSource = new String(Files.readAllBytes(Paths.get(getServletContext().getRealPath("/presearch.json"))), Charset.defaultCharset());
		String sSearchSource = new String(Files.readAllBytes(Paths.get(getServletContext().getRealPath("/search.json"))), Charset.defaultCharset());

		String preSource = String.format(sPreSource, query);
		SearchResponse preResp = client.prepareSearch("apps").setTypes("app").setSize(1)
				.setQuery(preSource).execute().actionGet();
		double maxScore = preResp.getHits().getMaxScore();
		if (preResp.getHits().getTotalHits() == 0) {
			out.println("<h2>Search returned no results.</2>");
			transportClient.close();
			return;
		}

		String searchSource = String.format(sSearchSource, query, maxScore);
		SearchResponse resp = client.prepareSearch("apps").setTypes("app").setQuery(searchSource).setSize(200).execute().actionGet();

		long toShow = resp.getHits().getTotalHits();
		out.println("<h2>Top " + (toShow > 200 ? 200 : toShow) + " results:</h2>");

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
			String numRating = (String) map.get("num ratings");
			String rating = (String) map.get("rating");
			String installs = (String) map.get("installs");
			String updated = (String) map.get("updated");
			String appScore = (String) map.get("appscore");

			StringBuilder builder = new StringBuilder(domains[0]);
			for (int i = 1; i < domains.length; ++i) {
				if (domains[i] != null)
					builder.append(", " + domains[1]);
				else
					break;
			}
			String domString = builder.toString();

			out.println("<p>");
			out.println("<b><i>Result " + res + ":</i></b><br/>");
			out.println("<b>TITLE:</b>     " + title + "<br/>");
			out.println("<b>PUBLISHER:</b> " + publisher + "<br/>");
			out.println("<b>DOMAINS:</b>   " + domString + "<br/>");
			out.println("<b>CATEGORY:</b>  " + category + "<br/>");
			out.println("<b>RATING:</b>    " + rating + " (by " + numRating + ")<br/>");
			out.println("<b>INSTALLS:</b>  " + installs + "<br/>");
			out.println("<b>LAST UPDATED:</b> " + updated + "<br/>");
			out.println("<b>DESCRIPTION:</b><br/>");
			out.println("   " + description + "<br/>");
			out.println("<b><i>APP SCORE (debug):</i></b>  " + appScore + "<br/>");
			out.println("<b><i>ES SCORE (debug):</i></b>   " + hit.getScore() + "<br/>");
			out.println("</p>");
			++res;
		}

		transportClient.close();
	}

}
