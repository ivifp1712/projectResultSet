package org.example;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class App 
{
    public static void main( String[] args )
    {

        while(true){
            Scanner scOpc = new Scanner(System.in);
            System.out.println("Elige una opción:");
            System.out.println("1. Mysql");
            System.out.println("2. Postgresql");
            System.out.println("3. Salir");
            String sc44 = scOpc.next();
            BDatos bdUsuarios = null;
            BDatos bdFacturas = null;
            if (sc44.equals("1")){
                bdUsuarios = new BDatos("PRUEBA", "root", "curso", "usuarios", "mysql");
                bdFacturas = new BDatos("PRUEBA", "root", "curso", "facturas", "mysql");
            }else if (sc44.equals("2")) {
                bdUsuarios = new BDatos("prueba", "ivi", "ivi", "usuarios", "postgre");
                bdFacturas = new BDatos("prueba", "ivi", "ivi", "facturas", "postgre");
            }else if (sc44.equals("3")) {
                System.exit(0);
            }
            else{
                System.out.println("Opción incorrecta");
            }

            boolean flag = false;
            boolean flag2 = false;
            while (!flag && !flag2) {
                System.out.println("¿Desea acceder a los usuarios o a las facturas?");
                System.out.println("1. Usuarios");
                System.out.println("2. Facturas");
                String opc2 = scOpc.next();

                if (opc2.equals("1")) {
                    flag = true;
                } else if (opc2.equals("2")) {
                    flag2 = true;
                } else {
                    System.out.println("No se ha introducido correctamente. Inténtelo de nuevo.");
                }
            }

            while(flag){
                Scanner sc = new Scanner(System.in);
                System.out.println("Tabla de usuarios");
                System.out.println("¿Que accion desea realizar?\n1. Añadir.\n" +
                        "2. Listado.\n" +
                        "3. Modificar.\n" +
                        "4. Exportar CSV \n" +
                        "5. Importar CSV \n" +
                        "6. Salir");
                String opc = sc.next();
                //Cambiar base de datos y contraseña
                //BDatos bd = new BDatos("PRUEBA", "root", "curso", "usuarios");
                switch (opc){
                    case "1":
                        System.out.println("Introduzca el nombre, apellidos y NIF separados por comas");
                        System.out.println("Recuerda que el concepto ni ningun campo puede ir separado por espacios");
                        String s = sc.next();
                        List<String> entrada = new ArrayList<String>(Arrays.asList(s.split(",")));
                        try {
                            System.out.println("nombre: "+ entrada.get(0));
                            System.out.println("apellidos: "+ entrada.get(1));
                            System.out.println("NIF: "+ entrada.get(2));
                            bdUsuarios.insertar(entrada);
                        }catch (IndexOutOfBoundsException e){
                            System.out.println("No se ha introducido correctamente");
                        }
                        break;

                    case "2":
                        Scanner sc1 = new Scanner(System.in);
                        System.out.println("¿Desea ver una o todas?");
                        String res = sc1.next();
                        if (res.equals("una") || res.equals("Una")){
                            System.out.println("Escriba la condicion separadas por coma. EJ: nombre,ivan");
                            System.out.println("Recuerde, puede buscar nombre, apellidos, NIF");
                            String bus = sc1.next();
                            List<String> entrada1 = new ArrayList<String>(Arrays.asList(bus.split(",")));
                            try {
                                ResultSet rs = bdUsuarios.verUna(entrada1);
                                System.out.println("NUM\tNOMBRE\t\tAPELLIDOS\tNIF\t");
                                while (rs.next()){
                                    System.out.print(rs.getInt("ID") + "\t");
                                    System.out.print(rs.getString("NOMBRE") + "\t");
                                    System.out.print(rs.getString("apellidos") + "\t");
                                    System.out.print(rs.getString("NIF") + "\t");
                                    System.out.println("");
                                }
                            } catch (SQLException e) {
                                System.out.println("Error al mostrar los datos");
                            }
                            break;
                        }
                        try {
                            ResultSet rs = bdUsuarios.verTodos();
                            System.out.println("ID\tNOMBRE\t\tAPELLIDOS\tNIF\t");
                            while (rs.next()){
                                System.out.print(rs.getInt("ID") + "\t");
                                System.out.print(rs.getString("NOMBRE") + "\t");
                                System.out.print(rs.getString("apellidos") + "\t");
                                System.out.print(rs.getString("NIF") + "\t");
                                System.out.println("");
                            }
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "3":
                        Scanner sc2 = new Scanner(System.in);
                        System.out.println("Escriba la condicion separadas por coma. EJ: nombre,ivan");
                        String cond = sc2.next();
                        System.out.println("Escriba que quiera cambiar separado por coma. EJ: nombre,ivan");
                        String camb = sc2.next();
                        List<String> entrada1 = new ArrayList<String>(Arrays.asList(cond.split(",")));
                        List<String> entrada2 = new ArrayList<String>(Arrays.asList(camb.split(",")));
                        bdUsuarios.modificar(entrada1, entrada2);
                        break;
                    case "4":
                        // Exportar
                        System.out.println("Introduzca la ruta absoluta");
                        Scanner sc8 = new Scanner(System.in);
                        String rutaExportar = sc8.nextLine();
                        bdUsuarios.exportarCSV(rutaExportar);
                        break;
                    case "5":
                        // Importar
                        System.out.println("Introduzca la ruta absoluta");
                        Scanner sc9 = new Scanner(System.in);
                        String rutaImportar = sc9.nextLine();
                        bdUsuarios.importarCSV(rutaImportar);
                        break;
                    case "6":
                        flag = false;
                        flag2 = false;
                        break;
                    default:
                        System.out.println("Introduce un numero correcto!");
                        break;
                }
            }

            while (flag2){
                Scanner sc = new Scanner(System.in);
                System.out.println("Tabla de facturas");
                System.out.println("¿Que accion desea realizar?\n1. Añadir.\n" +
                        "2. Listado.\n" +
                        "3. Modificar.\n" +
                        "4. Exportar CSV \n" +
                        "5. Importar CSV \n" +
                        "6. Salir");
                String opc = sc.next();
                //Cambiar base de datos y contraseña

                switch (opc){
                    case "1":
                        try {
                            ResultSet rs = bdFacturas.verTodos();
                            System.out.println("IDFactura\tidUsuario\tConcepto\tFecha\t");
                            while (rs.next()){
                                System.out.print(rs.getInt("idFactura") + "\t");
                                System.out.print(rs.getString("idUsuario") + "\t");
                                System.out.print(rs.getString("concepto") + "\t");
                                System.out.print(rs.getString("fecha") + "\t");
                                System.out.println("");
                            }
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                        System.out.println("Introduzca el id del usuario, concepto y fecha(formato 17-12-2003) por comas");
                        System.out.println("Recuerda que el concepto ni ningun campo puede ir separado por espacios");
                        String s = sc.next();
                        try {
                            List<String> entrada = new ArrayList<String>(Arrays.asList(s.split(",")));
                            System.out.println("nombre: "+ entrada.get(0));
                            System.out.println("concepto: "+ entrada.get(1));
                            System.out.println("fecha: "+ entrada.get(2));
                            System.out.println("Insertando...");
                            bdFacturas.insertar(entrada);
                        }catch (Exception e){
                            System.out.println("Error al introducir los datos");
                        }

                        break;

                    case "2":
                        Scanner sc1 = new Scanner(System.in);
                        System.out.println("¿Desea ver una o todas?");
                        String res = sc1.next();
                        if (res.equals("una") || res.equals("Una")){
                            System.out.println("Escriba la condicion separadas por coma. EJ: concepto,elmejorcoche");
                            System.out.println("Recuerde, puede buscar IDFactura, idUsuario, Concepto, Fecha\t");
                            String bus = sc1.next();
                            List<String> entrada1 = new ArrayList<String>(Arrays.asList(bus.split(",")));
                            try {
                                ResultSet rs = bdFacturas.verUna(entrada1);
                                System.out.println("IDFactura\tidUsuario\tConcepto\tFecha\t");
                                while (rs.next()){
                                    System.out.print(rs.getInt("idFactura") + "\t");
                                    System.out.print(rs.getString("idUsuario") + "\t");
                                    System.out.print(rs.getString("concepto") + "\t");
                                    System.out.print(rs.getString("fecha") + "\t");
                                    System.out.println("");
                                }
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            } catch (NullPointerException e){
                                System.out.println("No se ha encontrado ningun resultado");
                            }
                            break;
                        }
                        try {
                            ResultSet rs = bdFacturas.verTodos();
                            System.out.println("IDFactura\tidUsuario\tConcepto\tFecha\t");
                            while (rs.next()){
                                System.out.print(rs.getInt("idFactura") + "\t");
                                System.out.print(rs.getString("idUsuario") + "\t");
                                System.out.print(rs.getString("concepto") + "\t");
                                System.out.print(rs.getString("fecha") + "\t");
                                System.out.println("");
                            }
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    case "3":
                        Scanner sc2 = new Scanner(System.in);
                        System.out.println("Escriba la condicion separadas por coma. EJ: nombre,ivan");
                        String cond = sc2.next();
                        System.out.println("Escriba que quiera cambiar separado por coma. EJ: nombre,ivan");
                        String camb = sc2.next();
                        List<String> entrada1 = new ArrayList<String>(Arrays.asList(cond.split(",")));
                        List<String> entrada2 = new ArrayList<String>(Arrays.asList(camb.split(",")));
                        bdFacturas.modificar(entrada1, entrada2);
                        break;

                    case "4":
                        // Exportar
                        System.out.println("Introduzca la ruta absoluta");
                        Scanner sc8 = new Scanner(System.in);
                        String rutaExportar = sc8.nextLine();
                        bdFacturas.exportarCSV(rutaExportar);
                        break;
                    case "5":
                        // Importar
                        System.out.println("Introduzca la ruta absoluta");
                        Scanner sc9 = new Scanner(System.in);
                        String rutaImportar = sc9.nextLine();
                        bdFacturas.importarCSV(rutaImportar);
                        break;
                    case "6":
                        flag = false;
                        flag2 = false;
                        break;
                    default:
                        System.out.println("Introduce un numero correcto!");
                        break;
                }
            }
        }

    }
}
