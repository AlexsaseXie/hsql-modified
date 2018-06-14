/* Copyright (c) 2001-2011, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb.sample;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

/**
 * Title:        TestInsert
 * Description:  simple hello world db example of a
 *               standalone persistent db application
 *
 *               every time it runs it adds four more rows to sample_table
 *               it does a query and prints the results to standard out
 *
 * Author: Karl Meissner karl@meissnersd.com
 */
public class TestInsert {

    Connection conn;                                                //our connnection to the db - presist for life of program

    // we dont want this garbage collected until we are done
    public TestInsert(String db_file_name_prefix) throws Exception {    // note more general exception

        // Load the HSQL Database Engine JDBC driver
        // hsqldb.jar should be in the class path or made part of the current jar
        Class.forName("org.hsqldb.jdbc.JDBCDriver");

        // connect to the database.   This will load the db files and start the
        // database if it is not alread running.
        // db_file_name_prefix is used to open or create files that hold the state
        // of the db.
        // It can contain directory names relative to the
        // current working directory
        conn = DriverManager.getConnection("jdbc:hsqldb:"
                                           + "hsql://127.0.0.1:9001",    // filenames
                                           "SA",                     // username
                                           "");                      // password
    }

    public void shutdown() throws SQLException {

        Statement st = conn.createStatement();

        // db writes out to files and performs clean shuts down
        // otherwise there will be an unclean shutdown
        // when program ends
        st.execute("SHUTDOWN");
        conn.close();    // if there are no other open connection
    }

//use for SQL command SELECT
    public synchronized void query(String expression) throws SQLException {

        Statement st = null;
        ResultSet rs = null;

        st = conn.createStatement();         // statement objects can be reused with

        // repeated calls to execute but we
        // choose to make a new one each time
        rs = st.executeQuery(expression);    // run the query

        // do something with the result set.
        dump(rs);
        st.close();    // NOTE!! if you close a statement the associated ResultSet is

        // closed too
        // so you should copy the contents to some other object.
        // the result set is invalidated also  if you recycle an Statement
        // and try to execute some other query before the result set has been
        // completely examined.
    }
    

//use for SQL commands CREATE, DROP, INSERT and UPDATE
    public synchronized void update(String expression) throws SQLException {

        Statement st = null;

        st = conn.createStatement();    // statements

        //int i = st.executeUpdate(expression);    // run the query
        boolean p = st.execute(expression);

        if (p == false) {
            // System.out.println("db error : " + expression);
        }

        st.close();
    }    // void update()

    public static void dump(ResultSet rs) throws SQLException {

        // the order of the rows in a cursor
        // are implementation dependent unless you use the SQL ORDER statement
        ResultSetMetaData meta   = rs.getMetaData();
        int               colmax = meta.getColumnCount();
        int               i;
        Object            o = null;

        // the result set is a cursor into the data.  You can only
        // point to one row at a time
        // assume we are pointing to BEFORE the first row
        // rs.next() points to next row and returns true
        // or false if there is no next row, which breaks the loop
        for (; rs.next(); ) {
            for (i = 0; i < colmax; ++i) {
                o = rs.getObject(i + 1);    // Is SQL the first column is indexed

//                 with 1 not 0
                if (o == null) {
                	System.out.print("null ");
                } else {
                	System.out.print(o.toString() + " ");
                }
            }

            System.out.println(" ");
        }
    }                                       //void dump( ResultSet rs )

    public void dropTables() throws FileNotFoundException, SQLException {
    	updateFromFile("D:\\Desktop\\数据库\\HSQL优化\\datas\\drop-table.sql");
    }
    
    public void createTables() throws FileNotFoundException, SQLException {
    	updateFromFile("D:\\Desktop\\数据库\\HSQL优化\\datas\\create-table-cached.sql");
    }
    
    public void addKeys() throws FileNotFoundException, SQLException {
    	updateFromFile("D:\\Desktop\\数据库\\HSQL优化\\datas\\add-key.sql");
    }
    
    public void updateFromFile(String fileName) throws FileNotFoundException, SQLException {
    	Scanner scn = new Scanner(new FileReader(fileName));
    	String line = null;
    	int count = 0;
    	while (scn.hasNextLine()) {
    		line = scn.nextLine();
    		if (line.length() < 3) {
    			break;
    		}
    		update(line);
    		++count;
    		if (count % 1000 == 0) {
    			System.out.println(count);
    		}
    		//System.out.println(line);
    	}
    }
    
    public void inserts() throws FileNotFoundException, SQLException {
    	updateFromFile("D:\\Desktop\\数据库\\HSQL优化\\datas\\data.sql");
    }
    
    public static void main(String[] args) {

        TestInsert db = null;

        try {
            db = new TestInsert("db_file");
        } catch (Exception ex1) {
            ex1.printStackTrace();    // could not start db

            return;                   // bye bye
        }
        
        
        boolean drop = false;
        // drop, create, add
        try {
        	//db.dropTables();
        	
        	db.createTables();
        	db.addKeys();
        } catch (Exception e) {
        	e.printStackTrace();
        }

        try {

            // add some rows - will create duplicates if run more then once
            // the id column is automatically generated
//            db.update(
//                "INSERT INTO \"13219\".sample_table(str_col,num_col) VALUES('Ford', 100)");
//        	db.update("insert into person values ('8411151206', 'George Weasley', '5903051850', '5810201023');");

            // do a query
//            db.query("select * from person");
//        	

            // at end of program
            //db.shutdown();
//        	db.update("insert into tt values ('sample3');");
        	
        	//db.query("select * from tt;");
        	
        	//db.shutdown();
        	
        	db.inserts();
        	
        } catch (Exception e) {
        	e.printStackTrace();
        }
    }    // main()
}    // class TestInsert

