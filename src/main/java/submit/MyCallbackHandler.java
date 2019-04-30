package submit;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;

import java.util.List;

class MyCallbackHandler implements AMRMClientAsync.CallbackHandler {

    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses) {
        System.out.println("onContainersCompleted");
    }

    @Override
    public void onContainersAllocated(List<Container> containers) {
        System.out.println("containers size " + containers.size());
    }

    @Override
    public void onShutdownRequest() {
        System.out.println("onShutdownRequest");
    }

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {
        System.out.println("onNodesUpdated");
    }

    @Override
    public float getProgress() {
        return 0;
    }

    @Override
    public void onError(Throwable e) {
        System.out.println("onError");
    }
}