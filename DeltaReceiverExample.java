import java.util.List;
import java.util.ArrayList;

class DeltaReceiverExample implements DeltaReceiver {

    //Fields
    List<Delta> deltas;
    
    // Methods
    DeltaReceiverExample() { deltas = new ArrayList<>(); }
    
    @Override
    public void accept(List<Delta> deltas) {
        for (Delta delta : deltas) {
            this.deltas.add(delta);
        }

        System.out.print("{" + Thread.currentThread() + "} " + "[");
        for (Delta element : this.deltas) {
            System.out.print(element.getIdx() + " (" + element.getDataID() + "), ");
        }
        System.out.println("], length: " + deltas.size());

    }
}
