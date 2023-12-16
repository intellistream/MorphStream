public class OpenNFControllerTest {

    public static void main(String[] args) {
        OpenNFController masterController = new OpenNFController(4,4); // Create a master controller with 5 threads

        // Add VNFs and start controller threads for each VNF
        masterController.addVNF(0);
        masterController.addVNF(1);
        masterController.addVNF(2);
        masterController.addVNF(3);

        // Simulate receiving requests from VNFs
        for (int i = 0; i < 100; i++) {
            masterController.register_event_to_controller(0, "0,Request from VNF1");
            masterController.register_event_to_controller(1, "1,Request from VNF2");
            masterController.register_event_to_controller(2, "2,Request from VNF3");
            masterController.register_event_to_controller(3, "3,Request from VNF4");
        }
    }

}
