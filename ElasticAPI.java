/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package javaelastictutuorial.javaelastictutuorial;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.elasticsearch.index.query.QueryBuilder;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author Umut K.
 */
public class ElasticAPI {

    final private String SERVER = "http://localhost:9200/";


//------------------------------------------------------------------------------------------------------------------------------------------//
    //basic search in url paramter 
    public String dosearch(String database, String word) {
        String tmp = "";
        try {

            String url = SERVER + database + "/_search?q=" + word;
            System.out.println(url);

            URL object = new URL(url);
            HttpURLConnection con = (HttpURLConnection) object.openConnection();
            con.setDoOutput(true);
            con.setDoInput(true);
            con.setRequestProperty("Content-Type", "application/json");
            con.setRequestProperty("Accept", "application/json");
            con.setRequestMethod("POST");

            int HttpResult = con.getResponseCode();

            if (HttpResult == HttpURLConnection.HTTP_OK) {

                Reader in = new BufferedReader(new InputStreamReader(con.getInputStream(), "UTF-8"));

                for (int c; (c = in.read()) >= 0;) {
                    tmp += (char) c;

                }

                con.disconnect();
                return tmp;
            } else {
                System.out.println("HATA ALINDI");
                return tmp;
            }

        } catch (MalformedURLException ex) {
            Logger.getLogger(AdminLog.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(AdminLog.class.getName()).log(Level.SEVERE, null, ex);
        }
        return tmp;
    }
//------------------------------------------------------------------------------------------------------------------------------------------//
    //basic search with query
// words needs to be in _source
    public String doQuerysearch(String database, String word) {
        String tmp = "";
        try {
            QueryBuilder qb = queryStringQuery(word);

            Map<String, Object> params = new HashMap<>();
            params.put("query", qb);
            JSONObject cred = new JSONObject();
            params.entrySet().forEach((entry) -> {
                cred.put(entry.getKey(), entry.getValue());
            });

            String url = SERVER + database + "/_search/";
            System.out.println(url);
            System.out.println(cred);
            URL object = new URL(url);
            HttpURLConnection con = (HttpURLConnection) object.openConnection();
            con.setDoOutput(true);
            con.setDoInput(true);
            con.setRequestProperty("Content-Type", "application/json");
            con.setRequestProperty("Accept", "application/json");
            con.setRequestMethod("POST");

            OutputStreamWriter wr = new OutputStreamWriter(con.getOutputStream(), "UTF-8");

            wr.write(cred.toString());
            wr.flush();

            int HttpResult = con.getResponseCode();

            if (HttpResult == HttpURLConnection.HTTP_OK) {
                BufferedReader br = new BufferedReader(new InputStreamReader(
                        (con.getInputStream())));

                String output;
                System.out.println("Output from Server .... \n");

                while ((output = br.readLine()) != null) {
                    tmp += output;
                }

                wr.close();
                con.disconnect();
                return tmp;
            } else {
                System.out.println("HATA ALINDI");
                return tmp;
            }

        } catch (MalformedURLException ex) {
            Logger.getLogger(AdminLog.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(AdminLog.class.getName()).log(Level.SEVERE, null, ex);
        }
        return tmp;

    }
//------------------------------------------------------------------------------------------------------------------------------------------//
    //this for specific field name value from json its taking parameter as a string(return paramter from search)
    //I thought making json object can be faster than search to all document and if not i will change this to pattern and macher func.
       public long getvalue(String tmp) {
        long result = 0;
        try {

            JSONParser parser = new JSONParser();
            JSONObject json = (JSONObject) parser.parse(tmp);

            json = (JSONObject) json.get("hits");

            json = (JSONObject) json.get("total");

            result = (Long) json.get("value");
            return result;
        } catch (ParseException ex) {
            Logger.getLogger(ElasticAPI.class.getName()).log(Level.SEVERE, null, ex);
        }
        return result;
    }
  public boolean getsuccess(String tmp) {
        boolean result = false;
        try {

            JSONParser parser = new JSONParser();
            json = (JSONObject) parser.parse(tmp);

             json = (JSONObject) json.get("_shards");
        Long successful=(Long)json.get("successful");
       
            if (successful>0) {
                result=true;
            }
  
          return result;
        } catch (ParseException ex) {
            Logger.getLogger(ElasticAPI.class.getName()).log(Level.SEVERE, null, ex);
        }
        return result;
    }
  //------------------------------------------------------------------------------------------------------------------------------------------//
    //delete record with specific value and key (most likely we need to choose unique keys if we don't going to delete multiple record)
    public boolean deleteWithQuery(String database, String key, String Value) {
        boolean result = false;
        try {

            QueryBuilder qb = queryStringQuery(key);

            Map<String, Object> params = new HashMap<>();
            params.put(Value, qb);
            JSONObject cred = new JSONObject();
            cred.put(key, Value);
            JSONObject cred1 = new JSONObject();
            cred1.put("match", cred);
            JSONObject cred2 = new JSONObject();
            cred2.put("query", cred1);

            String url = SERVER + database + "/_delete_by_query/";
            System.out.println(url);
            System.out.println(cred2);
            URL object = new URL(url);
            HttpURLConnection con = (HttpURLConnection) object.openConnection();
            con.setDoOutput(true);
            con.setDoInput(true);
            con.setRequestProperty("Content-Type", "application/json");
            con.setRequestProperty("Accept", "application/json");
            con.setRequestMethod("POST");

            OutputStreamWriter wr = new OutputStreamWriter(con.getOutputStream(), "UTF-8");

            wr.write(cred2.toString());
            wr.flush();

            int HttpResult = con.getResponseCode();

            if (HttpResult == HttpURLConnection.HTTP_OK) {

                result = true;
                return result;
            }

        } catch (IOException ex) {
            Logger.getLogger(ElasticAPI.class.getName()).log(Level.SEVERE, null, ex);
        }
        return result;
    }
//------------------------------------------------------------------------------------------------------------------------------------------//
    public String addWithCtxScript(String database, String key, String fieldname, String Value) {

        String tmp = "";
        try {

            //key is lile a primary key you need to find your with spesifict key
            String url = SERVER + database + "/_update/" + key;
            JSONObject cred = new JSONObject();
            String sValue = "ctx._source." + fieldname + "=\'" + Value + "\'";
            cred.put("script", sValue);

            System.out.println(url);
            System.out.println(cred);
            URL object = new URL(url);
            HttpURLConnection con = (HttpURLConnection) object.openConnection();
            con.setDoOutput(true);
            con.setDoInput(true);
            con.setRequestProperty("Content-Type", "application/json");
            con.setRequestProperty("Accept", "application/json");
            con.setRequestMethod("POST");

            OutputStreamWriter wr = new OutputStreamWriter(con.getOutputStream(), "UTF-8");

            wr.write(cred.toString());
            wr.flush();

            int HttpResult = con.getResponseCode();

            if (HttpResult == HttpURLConnection.HTTP_OK) {
                BufferedReader br = new BufferedReader(new InputStreamReader(
                        (con.getInputStream())));

                String output;
                System.out.println("Output from Server .... \n");

                while ((output = br.readLine()) != null) {
                    tmp += output;
                }

                wr.close();
                con.disconnect();
                return tmp;
            } else {
                System.out.println("HATA ALINDI");
                return tmp;
            }

        } catch (MalformedURLException ex) {
            Logger.getLogger(AdminLog.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(AdminLog.class.getName()).log(Level.SEVERE, null, ex);
        }
        return tmp;
    }
//------------------------------------------------------------------------------------------------------------------------------------------//
   //key is lile a primary key you need to find your with spesifict key
    public String removeWithCtxScript(String database, String key, String fieldname) {
        String tmp = "";
        try {

            String url = SERVER + database + "/_update/" + key;
            JSONObject cred = new JSONObject();
            String sValue = "ctx._source.remove(\'" + fieldname + "\')";
            cred.put("script", sValue);
            System.out.println(cred);
            System.out.println(url);
            URL object = new URL(url);
            HttpURLConnection con = (HttpURLConnection) object.openConnection();
            con.setDoOutput(true);
            con.setDoInput(true);
            con.setRequestProperty("Content-Type", "application/json");
            con.setRequestProperty("Accept", "application/json");
            con.setRequestMethod("POST");

            OutputStreamWriter wr = new OutputStreamWriter(con.getOutputStream(), "UTF-8");

            wr.write(cred.toString());
            wr.flush();

            int HttpResult = con.getResponseCode();

            if (HttpResult == HttpURLConnection.HTTP_OK) {
                BufferedReader br = new BufferedReader(new InputStreamReader(
                        (con.getInputStream())));

                String output;
                System.out.println("Output from Server .... \n");

                while ((output = br.readLine()) != null) {
                    tmp += output;
                }

                wr.close();
                con.disconnect();
                return tmp;
            } else {
                System.out.println("HATA ALINDI");
                return tmp;
            }

        } catch (MalformedURLException ex) {
            Logger.getLogger(AdminLog.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(AdminLog.class.getName()).log(Level.SEVERE, null, ex);
        }
        return tmp;
    }
//------------------------------------------------------------------------------------------------------------------------------------------//
    //updating existing field
    public String updateWithQuery(String database, String key, String fieldname, String Value) {
        String tmp = "";
        try {
//key is lile a primary key you need to find your with spesifict key
//{"doc":{"isupdated":"yes"}}
            String url = SERVER + database + "/_update/" + key;
            JSONObject cred = new JSONObject();
            cred.put(fieldname, Value);
            JSONObject cred2 = new JSONObject();
            cred2.put("doc", cred);
            System.out.println(cred);
            System.out.println(url);
            URL object = new URL(url);
            HttpURLConnection con = (HttpURLConnection) object.openConnection();
            con.setDoOutput(true);
            con.setDoInput(true);
            con.setRequestProperty("Content-Type", "application/json");
            con.setRequestProperty("Accept", "application/json");
            con.setRequestMethod("POST");

            OutputStreamWriter wr = new OutputStreamWriter(con.getOutputStream(), "UTF-8");

            wr.write(cred2.toString());
            wr.flush();

            int HttpResult = con.getResponseCode();

            if (HttpResult == HttpURLConnection.HTTP_OK) {
                BufferedReader br = new BufferedReader(new InputStreamReader(
                        (con.getInputStream())));

                String output;
                System.out.println("Output from Server .... \n");

                while ((output = br.readLine()) != null) {
                    tmp += output;
                }

                wr.close();
                con.disconnect();
                return tmp;
            } else {
                System.out.println("HATA ALINDI");
                return tmp;
            }

        } catch (MalformedURLException ex) {
            Logger.getLogger(AdminLog.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(AdminLog.class.getName()).log(Level.SEVERE, null, ex);
        }
        return tmp;

    }
    //------------------------------------------------------------------------------------------------------------------------------------------//
}
