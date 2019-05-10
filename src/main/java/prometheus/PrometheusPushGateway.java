package prometheus;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;

import java.io.IOException;

public class PrometheusPushGateway {

    static final Gauge inprogressRequests = Gauge.build()
            .name("inprogress_requests").help("Inprogress requests.").create();


    public static void main(String[] args) throws IOException {
        PushGateway pushGateway = new PushGateway("");


//        pushGateway.delete("myJob93f5684cdfbad8fddaedbd5c0184cf96");
//        System.out.println("success");

        inprogressRequests.inc();

        Thread thread = new Thread();


        CollectorRegistry defaultRegistry = new CollectorRegistry(true);


        defaultRegistry.register(inprogressRequests);

        pushGateway.push(defaultRegistry, "mytestjob");


        System.out.println("aaa");
    }
}
