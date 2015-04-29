package com.keedio.storm;

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
	public void findGrupoPattern() {
		
		String pattern = "(<date>[^\\s]+)\\s+(<time>[^\\s]+)\\s+";
		String toFind = "hola amigo <date>11-23-24  <time>22:22:22 sflhsldfjs";
		
		Pattern patron = Pattern.compile(pattern);
		Matcher match = patron.matcher(toFind);

		int encontradas = 0;
		if (match.find()) {
			int count = match.groupCount();
			for (int i=1;i<=count;i++) {
				System.out.println(match.group(i));
				encontradas++;
			}
		}
		
		Assert.assertTrue("Encontramos dos fechas", 2==encontradas);
		

	}

}
