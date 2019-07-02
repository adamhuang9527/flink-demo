package submit;

import org.apache.commons.io.FileUtils;
import org.apache.http.HttpEntity;
import org.apache.http.ParseException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.File;
import java.io.IOException;

public class CreateGrafanaDashboard {
    public static void main(String[] args) throws IOException {
        CreateGrafanaDashboard c = new CreateGrafanaDashboard();
        c.create();


    }


    private void create() throws IOException {
        CloseableHttpClient httpclient = HttpClients.createDefault();


        HttpPost httpPost = new HttpPost("http://10.160.85.183:3000/api/dashboards/db");
        httpPost.addHeader("Authorization", "Bearer eyJrIjoiaHZEWTNqOXpIUDdJZUc3QzRQOTY5ZWJzbXRQamlnTlAiLCJuIjoidGVzdCIsImlkIjoxfQ==");
        httpPost.addHeader("Content-Type", "application/json");
        httpPost.addHeader("Accept", "application/json");


        String url = this.getClass().getResource("/grafana.json").getFile();
        String json = FileUtils.readFileToString(new File(url), "UTF-8");
        System.out.println(json);

        StringEntity stringEntity = new StringEntity(json, "UTF-8");
        stringEntity.setContentEncoding("UTF-8");

        httpPost.setEntity(stringEntity);

        CloseableHttpResponse response = httpclient.execute(httpPost);
        try {
            HttpEntity entity = response.getEntity();
            String result = null;
            try {
                result = EntityUtils.toString(entity);
            } catch (ParseException | IOException e) {
                e.printStackTrace();
            }
            System.out.println(result);
        } finally {
            response.close();
        }


    }
}
