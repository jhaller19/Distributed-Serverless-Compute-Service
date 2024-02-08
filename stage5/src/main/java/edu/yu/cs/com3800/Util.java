package edu.yu.cs.com3800;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;

public class Util {

    public static byte[] readAllBytesFromNetwork(InputStream in)  {
        try {
            while (in.available() == 0) {
                try {
                    Thread.currentThread().sleep(500);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        catch(IOException e){
            e.printStackTrace();

        }
        return readAllBytes(in);
    }
    public static byte[] leaderReadAllBytes(InputStream in)  {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int numberRead;
        byte[] data = new byte[40960];
        try {
            if(in.available() == 0){
                return null;
            }
            Thread.sleep(1000);
            while (in.available() > 0 && (numberRead = in.read(data, 0, data.length)) != -1   ) {
                buffer.write(data, 0, numberRead);
            }
        }catch(IOException | InterruptedException e){
            e.printStackTrace();
        }
        return buffer.toByteArray();
    }
    public static byte[] readAllBytesFromNetworkWithTimeout(InputStream in, long timeout)  {
        long start = System.currentTimeMillis();
        try {
            while (in.available() == 0) {
                try {
                    Thread.currentThread().sleep(500);
                }
                catch (InterruptedException e) {
                }
                if(System.currentTimeMillis() - start >= timeout){
                    return null;
                }
            }
        }
        catch(IOException e){
            e.printStackTrace();
        }
        return readAllBytes(in);
    }

    public static byte[] readAllBytes(InputStream in) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int numberRead;
        byte[] data = new byte[40960];
        try {
            while (in.available() > 0 && (numberRead = in.read(data, 0, data.length)) != -1   ) {
                buffer.write(data, 0, numberRead);
            }
        }catch(IOException e){
            e.printStackTrace();
        }
        return buffer.toByteArray();
    }

    public static Thread startAsDaemon(Runnable run, String name) {
        Thread thread = new Thread(run, name);
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    public static String getStackTrace(Exception e){
        ByteArrayOutputStream bas = new ByteArrayOutputStream();
        PrintStream myErr = new PrintStream(bas,true);
        e.printStackTrace(myErr);
        myErr.flush();
        myErr.close();
        return bas.toString();
    }
    public static Message getMessageToSend(Message.MessageType type , byte[] receivedMessageNetworkPayload, String myHostName, int myPort, String hostNameThatImSendingTo, int portThatImSendingTo,int requestId, boolean error){
        Message receivedMessage = new Message(receivedMessageNetworkPayload);
        //    public Message(MessageType type, byte[] contents, String senderHost, int senderPort, String receiverHost, int receiverPort, long requestID, boolean errorOccurred) {
        return new Message(type, receivedMessage.getMessageContents(), myHostName, myPort,
                hostNameThatImSendingTo, portThatImSendingTo, requestId, error);

    }
}
