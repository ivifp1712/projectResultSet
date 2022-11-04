package org.example;



import com.opencsv.CSVReader;

import javax.swing.plaf.nimbus.State;
import java.io.*;
import java.sql.*;
import java.util.*;

public class BDatos {
    private String cadenaConexion = "jdbc:mysql://localhost:3306/";
    private String cadenaConexionposgre = "jdbc:postgresql://localhost:5432/";
    private String user = "root";
    private String pass = "amelia";
    private Connection con;
    private String tabla;
    public BDatos(String baseDatos, String user, String contra, String tabla, String bd) {
        this.cadenaConexion = this.cadenaConexion + baseDatos;
        this.cadenaConexionposgre = this.cadenaConexion + baseDatos;
        this.user = user;
        this.pass = contra;
        this.tabla = tabla;

        if (bd.equals("mysql")){
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
                this.con = DriverManager.getConnection(cadenaConexion, user, pass);
                this.con.setAutoCommit(false);
                //Statement sentencia = con.createStatement();
            } catch (SQLException e) {
                System.out.println("No se ha podido establecer la conexión con la BD");
                System.out.println(e.getMessage());}
            catch (ClassNotFoundException e) {
                System.out.println("No se ha encontrado el driver para MySQL");
                return;
            } catch (Exception e){System.out.println("ERROR GENERAL");}
        } else if (bd.equals("postgre")) {
            //Conexión a PostgreSQL
            try {
                Class.forName("org.postgresql.Driver");
                this.con = DriverManager.getConnection("jdbc:postgresql://localhost/prueba?user="+user+"&password="+pass);
                //this.con = DriverManager.getConnection(cadenaConexionposgre, user, pass);
                this.con.setAutoCommit(false);
                //Statement sentencia = con.createStatement();
            } catch (SQLException e) {
                System.out.println("No se ha podido establecer la conexión con la BD");
                System.out.println(e.getMessage());}
            catch (ClassNotFoundException e) {
                System.out.println("No se ha encontrado el driver para PostgreSQL");
                return;
            } catch (Exception e){System.out.println("ERROR GENERAL");}
        }else{
            System.out.println("ERROR EN RECONOCER EL TIPO DE BASE DE DATOS");
        }
    }

    public void insertar(List<String> entrada){
        if (this.tabla.equals("usuarios")){
            String nombre = entrada.get(0);
            String apellidos = entrada.get(1);
            String NIF = entrada.get(2);
            PreparedStatement sentencia = null;
            try {
                //sentencia = con.createStatement();
                //String sql = String.format("INSERT INTO usuarios (nombre,apellidos,NIF) VALUES ('%s', '%s', '%s')", nombre, apellidos, NIF);
                sentencia  = con.prepareStatement("INSERT INTO"+this.tabla+" (nombre,apellidos,NIF) VALUES (?,?,?)");
                sentencia.setString(1, nombre);
                sentencia.setString(2, apellidos);
                sentencia.setString(3, NIF);
                int afectados = sentencia.executeUpdate();
                con.commit();
                System.out.println("Sentencia SQL ejecutada con éxito");
                System.out.println("Registros afectados:  "+ afectados);
                System.out.println("Insertado con exito!");
            } catch (SQLException e) {
                try {
                    con.rollback();
                } catch (SQLException ex) {
                    throw new RuntimeException(ex);
                }
                throw new RuntimeException(e);
            }
        }else{
            PreparedStatement sentencia = null;
            String idusuario = entrada.get(0);
            String concepto = entrada.get(1);
            String fecha = entrada.get(2);
            try {
                //sentencia = con.createStatement();
                //String sql = String.format("INSERT INTO usuarios (nombre,apellidos,NIF) VALUES ('%s', '%s', '%s')", nombre, apellidos, NIF);
                sentencia  = con.prepareStatement("INSERT INTO "+this.tabla+" (idusuario, concepto,fecha) VALUES (?, ?,?)");
                sentencia.setString(1, idusuario);
                sentencia.setString(2, concepto);
                sentencia.setString(3, fecha);
                int afectados = sentencia.executeUpdate();
                con.commit();
                System.out.println("Sentencia SQL ejecutada con éxito");
                System.out.println("Registros afectados:  "+ afectados);
                System.out.println("Insertado con exito!");
            } catch (SQLException e) {
                try {
                    con.rollback();
                } catch (SQLException ex) {
                    throw new RuntimeException(ex);
                }
                throw new RuntimeException(e);
            }
        }


    }

    public ResultSet verTodos(){
        Statement sentencia = null;
        try {
            sentencia = con.createStatement();
            String sql = String.format("select * from "+this.tabla);
            ResultSet rs = sentencia.executeQuery(sql);
            return rs;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public ResultSet verUna(List<String> var){
        Statement sentencia = null;
        try {
            sentencia = con.createStatement();
            System.out.println(var.get(0));
            System.out.println(var.get(1));
            String sql = "select * from "+this.tabla+" where "+var.get(0)+" like '%"+var.get(1)+"%'";
            //String sql = String.format("select * from usuarios where %s like '%%s%'", var.get(0), var.get(1));
            ResultSet rs = sentencia.executeQuery(sql);
            return rs;
        } catch (SQLSyntaxErrorException e){
            System.out.println("Ha fallado la busqueda " + e);
            return null;
        } catch (SQLException e){
            System.out.println("Ha fallado la busqueda " + e);
            return null;
        }
    }

    public void modificar(List<String> bus, List<String> camb){
        Statement sentencia = null;
        try {
            sentencia = con.createStatement();
            //System.out.println(bus.get(0));
            //System.out.println(bus.get(1));
            String sql = String.format("update "+this.tabla+" set %s = '%s' where %s= '%s'", camb.get(0), camb.get(1),bus.get(0), bus.get(1));
            sentencia.executeUpdate(sql);
            con.commit();
            System.out.println("Cambio realizado con exito!");

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void exportarCSV(String ruta){
        System.out.println("ruta: " + ruta);
        File file = new File(ruta);
        Statement sentencia = null;
        try {
            System.out.println("Tabla: " + this.tabla);
            FileWriter fw = new FileWriter(file+"/"+this.tabla+".csv");
            sentencia = con.createStatement();
            ResultSet rs = sentencia.executeQuery("select * from " + this.tabla);
            con.commit();
            int cols = rs.getMetaData().getColumnCount();

            for(int i = 1; i <= cols; i ++){
                System.out.println(rs.getMetaData().getColumnName(i));
                fw.append(rs.getMetaData().getColumnLabel(i));
                if(i < cols) fw.append(',');
                else fw.append('\n');
            }

            while (rs.next()) {

                for(int i = 1; i <= cols; i ++){
                    fw.append(rs.getString(i));
                    if(i < cols) fw.append(',');
                }
                fw.append('\n');
            }
            fw.flush();
            fw.close();
            System.out.print("CSV File is created successfully.");
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    public void importarCSV(String ruta){

        if (this.tabla.equals("usuarios")){
            System.out.println("Desea borrar los datos anterirores o sobreescribirlos? (sobreescribir/añadir)");
            System.out.println("Recuerda que si introduce un dato que ya existe, se sobreescribirá");
            Scanner sc = new Scanner(System.in);
            String opcion = sc.nextLine();
            if (opcion.equals("añadir")){
                // Comprobar si existe en la tabla usuarios y si existe, actualizar
                // Si no existe, insertar
                try {
                    Statement sentencia = con.createStatement();
                    String sql = "select * from usuarios where id";
                    ResultSet rs = sentencia.executeQuery(sql);
                    con.commit();
                    System.out.println("Datos borrados con exito!");
                    String coma = ",";
                    CSVReader reader = new CSVReader(new FileReader(ruta));
                    String insertQuery = "Insert into "+this.tabla+" (id, nombre, apellidos, NIF) values (?,?,?,?)";
                    //PreparedStatement pstmt = con.prepareStatement(insertQuery);
                    String[] rowData = null;
                    int i = 1;
                    boolean flag = false;
                    boolean repe = false;
                    while((rowData = reader.readNext()) != null)
                    {
                        repe = false;
                        PreparedStatement pstmt = con.prepareStatement(insertQuery);
                        for (String data : rowData)
                        {

                            if (data.equals("id")){
                                flag = true;

                                break;
                            }
                            if (i == 5){
                                break;
                            }
                            while (rs.next()){
                                if (rs.getString("id").equals(data)){
                                    repe = true;
                                    System.out.println("id igual");
                                    break;
                                }
                            }
                            if (repe == true){
                                break;
                            }
                            System.out.println(data);
                            pstmt.setString(i++, data);
                            flag = false;
                        } // cierra for
                        if (!flag){
                            i = 1;
                            pstmt.executeUpdate();
                            con.commit();
                        }

                    }
                    System.out.println("Data Successfully Uploaded");
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
            }
            if (opcion.equals("sobreescribir")){
            // Comprobar si existe en la tabla usuarios y si existe, actualizar
            // Si no existe, insertar
            try {
                Statement sentencia = con.createStatement();
                String sql = "select * from usuarios where id";
                ResultSet rs = sentencia.executeQuery(sql);
                con.commit();
                String coma = ",";
                CSVReader reader = new CSVReader(new FileReader(ruta));
                String insertQuery = "Insert into "+this.tabla+" (id, nombre, apellidos, NIF) values (?,?,?,?)";
                //PreparedStatement pstmt = con.prepareStatement(insertQuery);
                String updateQuery = "update "+this.tabla+" set nombre = ?, apellidos = ?, NIF = ? where id = ?";
                String[] rowData = null;
                int i = 1;
                boolean flag = false;
                String repe = "0";
                while((rowData = reader.readNext()) != null)
                {
                    repe = "0";
                    PreparedStatement pstmt = con.prepareStatement(insertQuery);
                    PreparedStatement update = con.prepareStatement(updateQuery);
                    for (String data : rowData)
                    {

                        if (data.equals("id")){
                            flag = true;
                            break;
                        }
                        if (i == 5){
                            break;
                        }
                        if (repe.equals("0")){
                            while (rs.next()){
                                if (rs.getString("id").equals(data)){
                                    repe = rs.getString("id");
                                    System.out.println("id igual " + repe);
                                    break;
                                }
                            }
                        }

                        if (!repe.equals(data)){
                            System.out.println("Update " + i  + ": " + data);
                            System.out.println(data);
                            update.setString(i++, data);
                            flag = false;
                        }else if(repe.equals("0")){
                            System.out.println(data);
                            pstmt.setString(i++, data);
                            flag = false;
                        }
                    } // Cierra for
                    if (!flag){
                        i = 1;
                        System.out.println("Repe valor: " + repe);
                        if (repe.equals("0")){
                            pstmt.executeUpdate();
                            con.commit();
                        }else{
                            update.setString(4, repe);
                            update.executeUpdate();
                            con.commit();
                        }
                    }

                }
                System.out.println("Data Successfully Uploaded");
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }}

        if (this.tabla.equals("facturas")){
            System.out.println("Desea borrar los datos anterirores o sobreescribirlos? (sobreescribir/añadir)");
            System.out.println("Recuerda que si introduce un dato que ya existe, se sobreescribirá");
            Scanner sc = new Scanner(System.in);
            String opcion = sc.nextLine();
            if (opcion.equals("añadir")){
                // Comprobar si existe en la tabla usuarios y si existe, actualizar
                // Si no existe, insertar
                try {
                    Statement sentencia = con.createStatement();
                    String sql = "select * from facturas";
                    ResultSet rs = sentencia.executeQuery(sql);
                    con.commit();
                    System.out.println("Datos borrados con exito!");
                    String coma = ",";
                    CSVReader reader = new CSVReader(new FileReader(ruta));
                    String insertQuery = "Insert into "+this.tabla+" (idFactura, idUsuario, concepto, fecha) values (?,?,?,?)";
                    //PreparedStatement pstmt = con.prepareStatement(insertQuery);
                    String[] rowData = null;
                    int i = 1;
                    boolean flag = false;
                    boolean repe = false;
                    while((rowData = reader.readNext()) != null)
                    {
                        repe = false;
                        PreparedStatement pstmt = con.prepareStatement(insertQuery);
                        for (String data : rowData)
                        {

                            if (data.equals("idfactura")){
                                flag = true;

                                break;
                            }
                            if (i == 5){
                                break;
                            }
                            while (rs.next()){
                                if (rs.getString("idfactura").equals(data)){
                                    repe = true;
                                    System.out.println("id igual");
                                    break;
                                }
                            }
                            if (repe == true){
                                break;
                            }
                            System.out.println(data);
                            pstmt.setString(i++, data);
                            flag = false;
                        } // cierra for
                        if (!flag){
                            i = 1;
                            pstmt.executeUpdate();
                            con.commit();
                        }

                    }
                    System.out.println("Data Successfully Uploaded");
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
            }
            if (opcion.equals("sobreescribir")){
                // Comprobar si existe en la tabla usuarios y si existe, actualizar
                // Si no existe, insertar
                try {
                    Statement sentencia = con.createStatement();
                    String sql = "select * from facturas";
                    ResultSet rs = sentencia.executeQuery(sql);
                    con.commit();
                    String coma = ",";
                    CSVReader reader = new CSVReader(new FileReader(ruta));
                    String insertQuery = "Insert into "+this.tabla+" (idFactura, idUsuario, concepto, fecha) values (?,?,?,?)";
                    //PreparedStatement pstmt = con.prepareStatement(insertQuery);
                    String updateQuery = "update "+this.tabla+" set idUsuario = ?, concepto = ?, fecha = ? where idFactura = ?";
                    String[] rowData = null;
                    int i = 1;
                    boolean flag = false;
                    String repe = "0";
                    boolean first = true;
                    while((rowData = reader.readNext()) != null)
                    {
                        repe = "0";
                        PreparedStatement pstmt = con.prepareStatement(insertQuery);
                        PreparedStatement update = con.prepareStatement(updateQuery);
                        first = true;
                        for (String data : rowData)
                        {

                            if (data.equals("idfactura")){
                                flag = true;
                                break;
                            }
                            if (i == 5){
                                break;
                            }
                            if (repe.equals("0")){
                                while (rs.next()){
                                    if (rs.getString("idfactura").equals(data)){
                                        repe = rs.getString("idfactura");
                                        System.out.println("id igual " + repe);
                                        break;
                                    }
                                }
                            }

                            if (!first && !repe.equals("0")){
                                System.out.println("Update " + i  + ": " + data);
                                System.out.println(data);
                                update.setString(i++, data);
                                flag = false;
                            }else if(repe.equals("0")) {
                                System.out.println(data);
                                pstmt.setString(i++, data);
                                flag = false;
                            }
                            if (first){
                                first = false;
                            }
                        } // Cierra for
                        if (!flag){
                            i = 1;
                            System.out.println("Repe valor: " + repe);
                            if (repe.equals("0")){
                                pstmt.executeUpdate();
                                con.commit();
                            }else{
                                update.setString(4, repe);
                                update.executeUpdate();
                                con.commit();
                            }
                        }

                    }
                    System.out.println("Data Successfully Uploaded");
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
            }
        }
    }
}