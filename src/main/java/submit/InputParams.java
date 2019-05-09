package submit;


import java.net.URL;
import java.util.ArrayList;
import java.util.List;


/**
 * 用户提交程序的输入参数类，其中jobName、classpaths、jarFilePath为必填项,
 * 其他为选填项，没有设置值的时候会为其填充默认值
 */
public class InputParams {

    /**
     * 必填
     */
    private String jobName;

    /**
     * 用户提交的jar的路径集合,必填，
     * URL对象可以通过new URL("path")来获取
     */
    private List<URL> classpaths = new ArrayList<>();

    /**
     * 用户程序的核心jar包路径，必填
     */
    private String jarFilePath;


    /**
     * 从指定savepoint启动
     */
    private String fromSavepoint;
    /**
     * job的并行度设置，默认1
     */
    private Integer parallelism;

    /**
     * user  jar path
     */
    private List<String> userJarPath = new ArrayList<>();

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
    private boolean allowNonRestoredState;


    /**
     * Memory for JobManager,单位是M
     */
    private int masterMemoryMB;

    /**
     * taskmanage内存，单位是M
     */
    private int taskManagerMemoryMB;

    /**
     * taskManager数量
     */
    private int numberTaskManagers;

    private int slotsPerTaskManager;

    public int getSlotsPerTaskManager() {
        return slotsPerTaskManager;
    }

    public void setSlotsPerTaskManager(int slotsPerTaskManager) {
        this.slotsPerTaskManager = slotsPerTaskManager;
    }

    public int getMasterMemoryMB() {
        return masterMemoryMB;
    }

    public void setMasterMemoryMB(int masterMemoryMB) {
        this.masterMemoryMB = masterMemoryMB;
    }

    public int getTaskManagerMemoryMB() {
        return taskManagerMemoryMB;
    }

    public void setTaskManagerMemoryMB(int taskManagerMemoryMB) {
        this.taskManagerMemoryMB = taskManagerMemoryMB;
    }

    public int getNumberTaskManagers() {
        return numberTaskManagers;
    }

    public void setNumberTaskManagers(int numberTaskManagers) {
        this.numberTaskManagers = numberTaskManagers;
    }

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


    public void setClasspaths(List<URL> classpaths) {
        this.classpaths = classpaths;
    }

    public List<String> getUserJarPath() {
        return userJarPath;
    }

    public void setUserJarPath(List<String> userJarPath) {
        this.userJarPath = userJarPath;
    }

    public List<URL> getClasspaths() {
        return classpaths;
    }
}
