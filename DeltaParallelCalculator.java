/**
 * Interfejs narzÄdzia wyznaczajÄcego rĂłĹźnice pomiÄdzy
 * zestawami danych.
 *
 */
public interface DeltaParallelCalculator {
	/**
	 * Metoda ustala liczbÄ wÄtkĂłw jaka ma byÄ uĹźyta do liczenia
	 * delty.
	 *
	 * @param threads liczba wÄtkĂłw.
	 */
	public void setThreadsNumber(int threads);

	/**
	 * Przekazany jako parametr obiekt ma byÄ uĹźywany
	 * do przekazywania za jego pomocÄ rezultatu.
	 *
	 * @param receiver obiekt odbierajÄcy wyniki
	 */
	public void setDeltaReceiver(DeltaReceiver receiver);

	/**
	 * Przekazanie danych do przetworzenia.
	 *
	 * @param data obiekt z danymi do przetworzenia
	 */
	public void addData(Data data);
}