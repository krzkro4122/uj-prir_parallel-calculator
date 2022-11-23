import java.util.List;
import java.util.ArrayList;

class DeltaReceiverExample implements DeltaReceiver {

    //Fields
    private final List<Delta> deltas;

    // Methods
    DeltaReceiverExample() { deltas = new ArrayList<>(); }

    @Override
    public void accept(List<Delta> deltas) {

        for (Delta delta : deltas) {
            this.deltas.add(delta);
        }

       { // print TODO
            String output = "";

            output += "\t[" + Thread.currentThread().getId() + "] GOTTEN DELTAS:";

            for (Delta delta : deltas)
                output += "\n\t\t{deltaID: " + delta.getDataID() +
                    ", deltaIndex: " + delta.getIdx() +
                    ", deltaValue: " + delta.getDelta() + "" + "}";

            System.out.println(output + "\n\t\tList<Delta>'s length: " + deltas.size());
        }

    }
}
