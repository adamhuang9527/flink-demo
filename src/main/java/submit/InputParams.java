package submit;


import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class InputParams {

    private String jobName;
    /**
     * 从指定savepoint启动
     */
    private String fromSavepoint;/
    /**
     * job的并行度设置，默认1
     */
    private Integer parallelism;
    /**
     * jar path
     */
    private String jarFilePath;
    /**
     * 类入口
     */
    private String entryPointClass;
    /**
     * 参数
     */
    private String[] programArgs;
    /**
     *
     */
    private List<URL> classpaths = new ArrayList<URL>();
    /**
     *
     */
    private boolean allowNonRestoredState;

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public boolean isAllowNonRestoredState() {
        return allowNonRestoredState;
    }

    public void setAllowNonRestoredState(boolean allowNonRestoredState) {
        this.allowNonRestoredState = allowNonRestoredState;
    }

    public String getFromSavepoint() {
        return fromSavepoint;
    }

    public void setFromSavepoint(String fromSavepoint) {
        this.fromSavepoint = fromSavepoint;
    }

    public Integer getParallelism() {
        return parallelism;
    }

    public void setParallelism(Integer parallelism) {
        this.parallelism = parallelism;
    }

    public String getJarFilePath() {
        return jarFilePath;
    }

    public void setJarFilePath(String jarFilePath) {
        this.jarFilePath = jarFilePath;
    }

    public String getEntryPointClass() {
        return entryPointClass;
    }

    public void setEntryPointClass(String entryPointClass) {
        this.entryPointClass = entryPointClass;
    }

    public String[] getProgramArgs() {
        return programArgs;
    }

    public void setProgramArgs(String[] programArgs) {
        this.programArgs = programArgs;
    }

    public List<URL> getClasspaths() {
        return classpaths;
    }

    public void setClasspaths(List<URL> classpaths) {
        this.classpaths = classpaths;
    }
}
