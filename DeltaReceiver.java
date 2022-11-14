import java.util.List;

/**
 * Interfejs odbiorcy danych
 *
 */
public interface DeltaReceiver {
	/**
	 * Do tej metody naleĹźy dostarczyÄ listÄ wykrytych rĂłĹźnic pomiÄdzy danymi
	 * z rĂłĹźnych zestawĂłw.
	 *
	 * @param deltas lista rĂłĹźnic
	 */
	public void accept( List<Delta> deltas );
}