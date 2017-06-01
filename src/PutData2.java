

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;

import com.example.kafka.kafkaProducer;
import com.metroinsight.geomesa.GeomesaHbase;
import com.metroinsight.geomesa.GeomesaHbase2;

/**
 * Servlet implementation class PutData
 */
@WebServlet("/PutData2")
public class PutData2 extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public PutData2() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		//response.getWriter().append("Served at: ").append(request.getContextPath());
		
		PrintWriter writer = response.getWriter();
		writer.println("<h1>Hello " + "MetroInsight is Running" + "</h1>");
		System.out.println("Hi, metroInsight is Running");
		
		 
        //querying Data now, results as shown below:
        System.out.println("querying Data now, results as shown below:");
        GeomesaHbase2 gmh=new GeomesaHbase2();
        JSONArray res=gmh.Query();
        
        writer.println("<p>Data is as below:<br> " +res.toJSONString()+ "<p>");
        
        System.out.println("Done");
        
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		//doGet(request, response);
		
		StringBuilder jsonBuff = new StringBuilder();
		String line = null;
		try {
		    BufferedReader reader = request.getReader();
		    while ((line = reader.readLine()) != null)
		        jsonBuff.append(line);
		} catch (Exception e) { /*error*/
			
			e.printStackTrace();
		}
	
		
		
		System.out.println("Received inputdata is:"+jsonBuff);
		
		//kafkaProducer kp=new kafkaProducer();
		//kp.SendData();
		
		String data = jsonBuff.toString();
		
		GeomesaHbase2 gmh=new GeomesaHbase2();
		gmh.geomesa_insertData(data);
		
		PrintWriter writer = response.getWriter();
		writer.println("<h1>Hello " + "Sandy Data Send" + "</h1>");
		writer.close();
		
	}

}
