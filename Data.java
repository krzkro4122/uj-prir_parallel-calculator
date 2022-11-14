
/**
 * Interfejs pojedynczego zestawu danych
 *
 */
public interface Data {
	/**
	 * Numer zestawu danych. KaĹźdy zestaw danych ma unikalny numer. Zestawy
	 * numerowane sÄ od 0 wzwyĹź.
	 *
	 * @return liczba caĹkowita oznaczajÄca numer zestawu danych
	 */
	public int getDataId();

	/**
	 * Rozmiar zestawu danych. Poprawne indeksy dla danych mieszczÄ siÄ od 0 do
	 * getSize-1.
	 *
	 * @return liczba danych.
	 */
	public int getSize();

	/**
	 * Odczyt danej z podanego indeksu. Poprawne indeksy dla danych mieszczÄ siÄ od
	 * 0 do getSize-1.
	 *
	 * @param idx numer indeksu
	 * @return odczytana wartoĹÄ
	 */
	public int getValue(int idx);
}