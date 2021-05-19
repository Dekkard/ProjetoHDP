package resources;

import java.time.LocalDateTime;
import java.util.Random;

public class FilenameGen {
	public static String randGen(Integer length) {
		String chaos = "";
		for(int i=0;i<length;i++) {
			int opt = Math.abs(new Random().nextInt()%3);
			int arises = Math.abs(new Random().nextInt());
			if(opt == 0) {
				arises = 48+arises%10;
			} else if(opt == 1) {
				arises = 65+arises%24;
			} else if(opt == 2) {
				arises = 97+arises%24;
			}
			chaos += (char)arises;
		}
		return chaos;
	}
	private static String addZeros(int num) {
		return "0".repeat(2-String.valueOf(num).length()).concat(String.valueOf(num));
	}
	private static String incTimeGen(LocalDateTime time) {
		return addZeros(time.getHour())+":"+addZeros(time.getMinute())+":"+addZeros(time.getSecond());
	}
	private static String incDateGen(LocalDateTime date) {
		return addZeros(date.getDayOfMonth())+"-"+addZeros(date.getMonth().getValue())+"-"+date.getYear();
	}
	public static String dateGen(boolean incDate, boolean incTime) {
		LocalDateTime ldt = LocalDateTime.now();
		return (incDate?incDateGen(ldt):"")+(incDate&&incTime?"-":(!(incDate||incTime)?"none":""))+(incTime?incTimeGen(ldt):"");
	}
}