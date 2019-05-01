package com.citi;


import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;	

public class readingJSON {

	public static void main(String[] args) {
		String js="{\"empid\":\"1007581\",\"first_name\":\"RAM\",\"last_name\":\"Raj\",\"passport\":\"true\"}";
		
		  //JSON parser object to parse read file
        JSONParser jsonParser = new JSONParser();
         
        try
        {
        	
            String reader=js;
			//Read JSON file
            Object obj = jsonParser.parse(reader);
            JSONObject employee=(JSONObject) obj ;
          //Get employee first name
            String firstName = (String) employee.get("first_name");   
            System.out.println(firstName);
             
            //Get employee last name
            String lastName = (String) employee.get("last_name"); 
            System.out.println(lastName);
             
            //Get employee Number
            String empid = (String) employee.get("empid");
            int empnum=Integer.parseInt(empid);
                      
            System.out.println(empnum);
 
        }
        catch (ParseException e) {
            e.printStackTrace();
        }
    }
 
    
}