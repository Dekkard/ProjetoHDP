package resources;

public class RegexMatch {
		public String WordMatch(String word) {
		if(word.matches("(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)")) {
			return /*word+": */"IP";
		} else if(word.matches("(\\d+)\\/(\\w+|\\d+)\\/(\\d+):(\\d+):(\\d+):(\\d+)")) { 
			return /*word+": */"Timestamp";
		} else if(word.matches("(\\d+)\\/(\\w+|\\d+)\\/(\\d+)")) { 
			return /*word+": */"Date";
		} else if(word.matches("(\\d+):(\\d+):(\\d+)")) { 
			return /*word+": */"Time";
		} else if(word.matches("([-+](\\d{4}))")) {
			return /*word+": */"GMT";
		} else if(word.matches("(GET|POST|PUT|DELETE)")) {
			return /*word+": */"Method";
		} else if(word.matches("(HTTP/(\\d+)\\.(\\d+))")) {
			return /*word+": */"Status";
		} else if(word.matches("(\\d{3})")) {
			int c = Integer.parseInt(word);
			if(c >= 100 && c < 200) {
				return /*word+": */"Code (Info)";
			} else if(c >= 200 && c < 300) {
				return /*word+": */"Code (Success)";
			} else if(c >= 300 && c < 400) {
				return /*word+": */"Code (Redirect)";
			} else if(c >= 400 && c < 500) {
				return /*word+": */"Code (Error - Client)";
			} else if(c >= 500 && c < 600) {
				return /*word+": */"Code (Error - Server)";
			}
		} else {
			return /*word+": */"URL";
		}
		return null;
	}
}
