/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ir.ac.itrc.rotbenegar.Utilities;

public class Logger {
    
    public static void write (String msg, Exception e) {
        System.out.println(msg + e.getMessage());
    }

    public static void write (String msg) {
        System.out.println(msg);
    }
}
