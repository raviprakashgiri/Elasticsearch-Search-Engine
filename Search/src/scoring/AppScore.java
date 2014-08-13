package scoring;

import java.util.List;

public class AppScore {
	
	static double cleanNumeral(String s, double def) {
		if (s == null)
			return def;
		String[] tokens = s.split("-");
		if (tokens.length > 2)
			return def;
		double total = 0;
		for (String t : tokens) {
			double v, mul = 0.0;
			t = t.replaceAll(",", "");
			t = t.replaceAll(" ", "");
			char last = t.substring(t.length() - 1, t.length()).toLowerCase().charAt(0);
			if (last == 'b') {
				mul = 1.0/(1024.0 * 1024.0);
			} else if (last == 'k') {
				mul = 1.0/1024.0;
			} else if (last == 'm') {
				mul = 1.0;
			} else if (last == 'g') {
				mul = 1024.0;
			}
			if (mul == 0.0)
				mul = 1.0;
			else
				t = t.substring(0, t.length() - 1);
			if (t.isEmpty()) {
				v = def;
			} else {
				try {
					v = Double.parseDouble(t) * mul;
				} catch (NumberFormatException e) {
					v = def;
				}
			}
			total += v;
		}
		return total / tokens.length;
	}

	public static void processRow(List<String> row, int numRatingIndex, int ratingIndex, int installsIndex, int sizeIndex) {

		double ratingScore, popularityScore, sizeScore, topDevScore, usageScore, mentionScore, appScore;
		
		int numRating = (int) cleanNumeral(row.get(numRatingIndex), 0);
		double ratings = cleanNumeral(row.get(ratingIndex), 3);
		int installs = (int) cleanNumeral(row.get(installsIndex), 0);
		double size = cleanNumeral(row.get(sizeIndex), 25);
		boolean isTopDeveloper = false; //dummy
		double[] usageData = {0.75, 0.75}; //dummy
		int blogMentions = 0; //dummy

		ratingScore = ((((ratings-3) / (1 + Math.abs(ratings-3))) * Math.log(1 + (Math.abs(ratings-3) * numRating))) + 11.75) / 23.5;
		popularityScore = Math.log(1 + installs) / 20.03;
		sizeScore = 1.98 / (1.0 + (Math.pow(Math.E, ((size - 25.0) / 1350.0))));
		topDevScore = isTopDeveloper ? 1.0 : 0.0;
		usageScore = 0.0;
		for (double d : usageData)
			usageScore += d;
		usageScore /= (double) usageData.length;
		mentionScore = Math.log(1.0 + blogMentions) / 4.75;
		
		appScore = (ratingScore + 0.725*popularityScore + 0.235*sizeScore + 0.3*topDevScore + 0.75*usageScore + 0.32*mentionScore) / 3.33;
		row.add(String.valueOf(appScore));
	}
}