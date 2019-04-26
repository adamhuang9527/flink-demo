package submit;


import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class InputParams {

    private String jarFilePath;
    private String entryPointClass;
    private String[] programArgs;
    private List<URL> classpaths = new ArrayList<URL>();

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
