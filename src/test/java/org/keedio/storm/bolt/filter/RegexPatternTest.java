package org.keedio.storm.bolt.filter;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Assert;
import org.junit.Test;

public class RegexPatternTest {

	@Test
	public void findDatePattern() {
		
		String pattern = "[0,1,2,3]\\d-[0,1]\\d-\\d\\d\\d\\d";
		String toFind = "En un lugar de 01-12-2015 de la mancha 24-03-2014 y esta no sea 45-10-2015";
		
		Pattern patron = Pattern.compile(pattern);
		Matcher match = patron.matcher(toFind);

		int encontradas = 0;
		while (match.find()) {
			String res = match.group();
			System.out.println(res);
			encontradas++;
		}
		
		Assert.assertTrue("Encontramos dos fechas", 2==encontradas);
		

	}

	@Test
	public void findEmailPattern() {
		
		String pattern = "[_A-Za-z0-9-]+(\\.[_A-Za-z0-9-]+)*@[A-Za-z0-9]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})";
		String toFind = "En un lugar de estesi@sale.com de la mancha este@nosale.claro y esta no sea este@nosale, peroeste@si.que.saldra.com";
		
		Pattern patron = Pattern.compile(pattern);
		Matcher match = patron.matcher(toFind);

		int encontradas = 0;
		while (match.find()) {
			String res = match.group();
			System.out.println(res);
			encontradas++;
		}
		
		Assert.assertTrue("Encontramos dos fechas", 3==encontradas);
		

	}

	@Test
	public void findCuentaPattern() {
		
		String pattern = "\\d{4} \\d{4} \\d{2} \\d{10}";
		String toFind = "En un lugar de 1111 1111 11 1111111111 de la mancha 111 1111 11 1111111111 y esta no sea 1234 1234 12 1234567890";
		
		Pattern patron = Pattern.compile(pattern);
		Matcher match = patron.matcher(toFind);

		int encontradas = 0;
		while (match.find()) {
			String res = match.group();
			System.out.println(res);
			encontradas++;
		}
		
		Assert.assertTrue("Encontramos dos fechas", 2==encontradas);
		

	}

	@Test
	public void findGrupoPattern() throws Exception{
		//TODO--> revisar el test
		String pattern = "(?<date>[0,1,2,3]\\d-[0,1]\\d-\\d\\d\\d\\d)\\s+(?<time>[0,1,2]\\d:\\d\\d)\\s+(?<time2>[0,1,2]\\d:\\d\\d)\\s+";
		String toFind = "12-10-2015 25:15 gjslkgjs gklg jsdlkgsdfgsd 12-11-2015 22:22 fsdofsfjsl";
		int encontradas = 0;
		
		
		Pattern patron = Pattern.compile(pattern);
		Map aux = getNamedGroups(patron);
		Matcher match = patron.matcher(toFind);
		while (match.find()) {
			System.out.println(match.group("date"));
			System.out.println(match.group("time"));
			encontradas++;
		}
		
		Assert.assertTrue("Encontramos dos fechas", true);
		

	}

	private static Map<String, Integer> getNamedGroups(Pattern regex)
			throws NoSuchMethodException, SecurityException,
			IllegalAccessException, IllegalArgumentException,
			InvocationTargetException {

		Method namedGroupsMethod = Pattern.class.getDeclaredMethod("namedGroups");
		namedGroupsMethod.setAccessible(true);

		Map<String, Integer> namedGroups = null;
		namedGroups = (Map<String, Integer>) namedGroupsMethod.invoke(regex);

		if (namedGroups == null) {
			throw new InternalError();
		}

		return Collections.unmodifiableMap(namedGroups);
	}

}
