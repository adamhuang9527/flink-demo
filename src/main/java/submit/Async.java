package submit;

import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

import static org.apache.hadoop.yarn.client.api.async.AMRMClientAsync.createAMRMClientAsync;

public class Async {
    public static void main(String[] args) throws IOException, YarnException, InterruptedException {


        YarnConfiguration yarnConfiguration = new YarnConfiguration();

        AMRMClientAsync asyncClient = createAMRMClientAsync(1000, new MyCallbackHandler());
        asyncClient.init(yarnConfiguration);
        asyncClient.start();


//        String trackingUrl = "http://:8888/cluster/app/application_1556176336087_0052";

//        RegisterApplicationMasterResponse response = asyncClient
//                .registerApplicationMaster("10.160.85.185", 8032, "");
//        asyncClient.addContainerRequest(containerRequest);
//        asyncClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "aaaa", trackingUrl);
//        asyncClient.stop();

        Thread.sleep(1000000);
    }
}

