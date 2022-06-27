package com.compact.tools.hire;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

/**
 * Service Log Klasse
 */
public class Loggable extends HireSession
{
    //--------------------------------------------------------------------------------------------------------------------------------

    public Loggable()
    {
    }

    //--------------------------------------------------------------------------------------------------------------------------------

    //TODO C: auf log4j umsteigen
    /**
     * Logmeldung in Stdandard Logfile des Application Servers. Basiert auf dem
     * NCSA Log Format.
     * 
     * @param message
     *            die Logausgabe
     */
    public void log( final Object message )
    {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        // Aufrufende Klasse ermitteln
        String callingClass = stackTrace[2].getClassName().substring( stackTrace[2].getClassName().lastIndexOf( "." ) + 1 );
        System.out.println( this.getSessionId() + " [" + new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss.SSS" ).format( new Date() ) + "]=> Hire Service->"
                + callingClass + "::" + stackTrace[2].getMethodName() + "(): " + message );
    }

    /**
     * Logmeldung in Stdandard Logfile des Application Servers. Basiert auf dem
     * NCSA Log Format.
     * 
     * @param message
     *            die Logausgabe
     */
    public void logCron( final Object message )
    {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        // Aufrufende Klasse ermitteln
        String callingClass = stackTrace[2].getClassName().substring( stackTrace[2].getClassName().lastIndexOf( "." ) + 1 );
        System.out.println( "HIRE CRON" + " [" + new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss.SSS" ).format( new Date() ) + "]=> Hire Service->"
                + callingClass + "::" + stackTrace[2].getMethodName() + "(): " + message );
    }
    
    /**
     * Logmeldung in Standard Logfile des Application Servers. Basiert auf dem
     * NCSA Log Format. Ausgabe "called" mit Zeilennummer.
     */
    public void log()
    {
        try {
            StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
            // Aufrufende Klasse ermitteln
            String callingClass = stackTrace[2].getClassName().substring( stackTrace[2].getClassName().lastIndexOf( "." ) + 1 );
            System.out.println( this.getSessionId() + " [" + new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss.SSS" ).format( new Date() )
                    + "]=> Hire Service->" + stackTrace[2].getLineNumber() + " " + callingClass + "::" + stackTrace[2].getMethodName() + "(): "
                    + "called" );
        }
        catch( Exception e ) {
            e.printStackTrace();
        }
    }
    
    /**
     * Logmeldung in Standard Logfile des Application Servers. Basiert auf dem
     * NCSA Log Format. Ausgabe "called" mit Zeilennummer.
     */
    public static void logS( final Object message) {
        try {
        ServletRequestAttributes attr = null;
        try {
            attr = ( ServletRequestAttributes ) RequestContextHolder.currentRequestAttributes();
        }
        catch (Exception e) {}
        String sessionId = ( attr == null ) ? "HIRE CRON" : attr.getRequest().getSession().getId();
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        // Aufrufende Klasse ermitteln
        String callingClass = stackTrace[2].getClassName().substring( stackTrace[2].getClassName().lastIndexOf( "." ) + 1 );
        System.out.println( sessionId + " [" + new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss.SSS" ).format( new Date() )
                + "]=> Hire Service->" + callingClass + "::" + stackTrace[2].getMethodName() + "(): "
                + message );
        }
        catch( Exception e ) {
            e.printStackTrace();
        }
    }

    //--------------------------------------------------------------------------------------------------------------------------------
}
