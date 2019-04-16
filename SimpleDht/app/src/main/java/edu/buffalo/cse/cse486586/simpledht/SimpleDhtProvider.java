package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.CursorIndexOutOfBoundsException;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.ContentValues.TAG;

public class SimpleDhtProvider extends ContentProvider {

    private static final String CONTENT_AUTHORITY = "edu.buffalo.cse.cse486586.simpledht.provider";
    private String keyMsg="";
    private String valueMsg="";
    private   int num=0;
    private static MatrixCursor matrixCursorObject2;
    private FileInputStream inputFileStreamObj;
    private BufferedInputStream bufferedObj;
    private String[] tableColumns = {"key","value"};
    public static boolean flag = true;
    boolean flagStart=true;
    int cnn =0;
    static final String[] REMOTE_PORT = {"11108", "11112", "11116","11120","11124"};
    static final int SERVER_PORT = 10000;
    private String nodeId;
    HashMap<String, String> dhtNodes = new HashMap<String, String>();// hashed index  -  port number
    List<String> oneNode; //hashedindex ports
    String successorNode;
    String predecessorNode;
    String portt;


    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        /*
         * Calculate the port number that this AVD listens on.
         */
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        Log.e("My Port",myPort);
        portt = myPort;
        /*
        Create a server socket as well as a thread (AsyncTask) that listens on the server
         */
        try {
            nodeId = genHash(Integer.toString(Integer.parseInt(myPort)/2));// 5554
            Log.i("node",nodeId);
            if(myPort.equals("11108")){
                oneNode = new ArrayList<String>();
            }
            dhtNodes.put(nodeId, myPort);
            String ss = "join"+"-"+"11108"+"-"+nodeId+ "-" +myPort;
            // creating server task
            try {
                ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
                new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            } catch (IOException e) {
                Log.e(TAG, "Can't create a ServerSocket");
                e.printStackTrace();
//                return false;
            }
            //getting succesor and predecessor
            getSuccessorPredecessor(ss);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return true;
    }

    private void getSuccessorPredecessor(String msg) {
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
    }

    private void delFilesFromSuccessor(String str){
        String line=null;
        try{
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(dhtNodes.get(successorNode)));
            PrintStream printOut = null;
            BufferedReader inReader = null;
            printOut = new PrintStream(socket.getOutputStream());
            inReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            Log.e("to succ", "response from avd: "+portt+" type of query: "+str.split("\\-")[0]);
//            printOut.println("*-kl");
            printOut.println(str);
            printOut.flush();
            inReader.readLine();
            socket.close();
        }catch (SocketException se){
            se.printStackTrace();
        }
        catch (UnknownHostException e) {
            Log.e(TAG, "getFilesFrom Successor Unknown");
            e.printStackTrace();
        } catch (IOException e) {
            Log.e(TAG, "getFilesFrom Successor  socket IOException");
            e.printStackTrace();
        }
    }

    private int delAllFilesFromAvd(Uri uri){
        MatrixCursor matrixCursorObject = new MatrixCursor(tableColumns);
        String outPutMessage = "";
        boolean bool=false;
        File dir = getContext().getFilesDir();
        try{
            matrixCursorObject = new MatrixCursor(tableColumns);
            for(File file: dir.listFiles()){
                bool = file.delete();
                Log.i("deleting : ",file.getName()+ "   file deleted:    "+bool);
            }
            return 1;
        } catch (IllegalArgumentException iae) {
            Log.e(TAG, "IllegalArgumentException in query ");
            iae.printStackTrace();
        } catch (NullPointerException npe) {
            Log.e(TAG, "NullPointerException in query ");
            npe.printStackTrace();
        } catch (Exception e) {
            Log.e(TAG, "Failed to return the cursor object!");
            e.printStackTrace();
        }
        return 0;
    }

    private int delFileFromAvd(Uri uri, String selection) throws CursorIndexOutOfBoundsException {
        ////query(buildObj1,null, inMessage[2], null, inMessage[3]);
        MatrixCursor matrixCursorObject = new MatrixCursor(tableColumns);
        String keyHashed = "";
        try {
            keyHashed = genHash(selection);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        //check whether this avd contains file
        if(successorNode==null || predecessorNode==null || keyHashed.compareTo(nodeId) ==0 || ((nodeId.compareTo(predecessorNode)==0) && (keyHashed.compareTo(predecessorNode)>0 || keyHashed.compareTo(predecessorNode)<0))
                || (predecessorNode.compareTo(nodeId)>0 && keyHashed.compareTo(predecessorNode)>0)
                || (predecessorNode.compareTo(nodeId)>0 && keyHashed.compareTo(nodeId)<0)
                || (keyHashed.compareTo(nodeId) <0 && keyHashed.compareTo(predecessorNode)>0)){

            return delCursorOBJ(selection);
        }
        //request successor for file
        else {
            delFilesFromSuccessor("#-"+selection);
            return 1;
        }
    }

    private int delCursorOBJ(String selection){
        MatrixCursor matrixCursorObject = new MatrixCursor(tableColumns);
        String outPutMessage = "";
        File dir = getContext().getFilesDir();
        boolean bool=false;
        try{
            matrixCursorObject = new MatrixCursor(tableColumns);
            for(File file: dir.listFiles()){
                if(file.getName().equals(selection)) {
                    Log.i("del : ",file.getName());
                    bool = file.delete();
                    Log.i("del : "," file deleted:   "+bool);
                }
            }
            return 1;
        }catch (IllegalArgumentException iae) {
            Log.e(TAG, "IllegalArgumentException in query ");
            iae.printStackTrace();
        } catch (NullPointerException npe) {
            Log.e(TAG, "NullPointerException in query ");
            npe.printStackTrace();
        } catch (Exception e) {
            Log.e(TAG, "Failed to return the cursor object!");
            e.printStackTrace();
        }
        return 0;
    }

    private int delFilesfromAllServers(Uri uri){
        flagStart=false;
        //get all messages from this avd
        MatrixCursor matrixCursorObject = new MatrixCursor(tableColumns);
        delAllFilesFromAvd(uri);
        //get messages from successor avd
        if(successorNode!=null){
            String allFiles=null;
            if(true){
                delFilesFromSuccessor("*-kl");
            }
        }
        //return cursor object
        flagStart=true;
         return 1;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        MatrixCursor mxc  = new MatrixCursor(tableColumns);
        Log.e("delete",selection);
        if(selection.equals("*")){
            if(flagStart){
                delFilesfromAllServers(uri);
                return 1;
            }
            return 1;
        } else if(selection.equals("@")) {
            delAllFilesFromAvd(uri);
            return 1;
        } else {
            return delFileFromAvd(uri, selection);
        }
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        this.keyMsg =  values.get("key").toString();
        this.valueMsg  =  values.get("value").toString();
        FileOutputStream outputStream =   null;
        String keyHashed="";
        try {
            keyHashed = genHash(keyMsg);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        Log.i("in insert: ", "insert key: "+ keyMsg+ " hashed kymsg: "+keyHashed+ "  nodehashed is: "+nodeId+" predecessor: "+ predecessorNode+ "  successor is: "+ successorNode);
        if(successorNode==null || predecessorNode==null || keyHashed.compareTo(nodeId) ==0 || ((nodeId.compareTo(predecessorNode)==0) && (keyHashed.compareTo(predecessorNode)>0 || keyHashed.compareTo(predecessorNode)<0))
            || (predecessorNode.compareTo(nodeId)>0 && keyHashed.compareTo(predecessorNode)>0)
            || (predecessorNode.compareTo(nodeId)>0 && keyHashed.compareTo(nodeId)<0)
            || (keyHashed.compareTo(nodeId) <0 && keyHashed.compareTo(predecessorNode)>0)){
            try {
                outputStream = getContext().openFileOutput(this.keyMsg, Context.MODE_PRIVATE);
                outputStream.write(this.valueMsg.getBytes());
                outputStream.close();
                Log.i("key Insert","  port: "+portt+"  inserted key : "+keyMsg+"   key hashed value  "+keyHashed+"  value: "+valueMsg);
            } catch (IllegalArgumentException iae) {
                Log.e(TAG, "IllegalArgumentException in insert");
                iae.printStackTrace();
            } catch (IOException io) {
                Log.e(TAG, "IOException in insert");
                io.printStackTrace();
            }catch (Exception e) {
                Log.e(TAG, "Exception in insert");
                e.printStackTrace();
            }
            return uri;
        } else {
            String OpMsg = "insert"+"-"+dhtNodes.get(successorNode)+ "-"+keyMsg+"-"+valueMsg;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, OpMsg);
            Log.e("insert sending","message sent; "+keyMsg+"-"+valueMsg+"   sending from: "+portt+"  sending to: "+dhtNodes.get(successorNode));
        }
        return null;
    }

    private Cursor getAllFilesFromAvd(Uri uri){
        MatrixCursor matrixCursorObject = new MatrixCursor(tableColumns);
        String outPutMessage = "";
        File dir = getContext().getFilesDir();
        try{
            matrixCursorObject = new MatrixCursor(tableColumns);
            for(File file: dir.listFiles()){
                FileReader fileReader = new FileReader(file);
                BufferedReader bufferedReader = new BufferedReader(fileReader);
                StringBuffer stringBuffer = new StringBuffer();
                String line = bufferedReader.readLine();
                fileReader.close();
                matrixCursorObject.addRow(new String[]{file.getName(), line});
                cnn++;
                Log.i("querying -- : ",file.getName()+ "   -   "+outPutMessage);
            }
            return matrixCursorObject;
        } catch (IllegalArgumentException iae) {
            Log.e(TAG, "IllegalArgumentException in query ");
            iae.printStackTrace();
        } catch (NullPointerException npe) {
            Log.e(TAG, "NullPointerException in query ");
            npe.printStackTrace();
        } catch (Exception e) {
            Log.e(TAG, "Failed to return the cursor object!");
            e.printStackTrace();
        }
        return null;
    }

    private String getFilesFromSuccessor(String str){
        String line=null;
        try{
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(dhtNodes.get(successorNode)));

            PrintStream printOut = null;
            BufferedReader inReader = null;
            printOut = new PrintStream(socket.getOutputStream());
            inReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            Log.e("to succ", "response from avd: "+portt+" type of query: "+str.split("\\-")[0]);
            printOut.println(str);
            printOut.flush();
            line =  inReader.readLine();
            Log.e("from succ", "response came on avd: "+portt+" type of query: "+str.split("\\-")[0] + "  response is: "+line);
            socket.close();
        }catch (SocketException se){
            se.printStackTrace();
        }
        catch (UnknownHostException e) {
            Log.e(TAG, "getFilesFrom Successor Unknown");
            e.printStackTrace();
        } catch (IOException e) {
            Log.e(TAG, "getFilesFrom Successor  socket IOException");
            e.printStackTrace();
        }
        return line;
    }

    private Cursor getFilesfromAllServers(Uri uri){
        flagStart=false;
        //get all messages from this avd
        MatrixCursor matrixCursorObject = new MatrixCursor(tableColumns);
        matrixCursorObject =  (MatrixCursor) getAllFilesFromAvd(uri);
        //get messages from successor avd
        if(successorNode!=null){
            String allFiles=null;
            Log.e("calling succ","calling");
            if(true){
                allFiles = getFilesFromSuccessor("*-kl");
                Log.e("all files",allFiles);
                String[] msgArray = allFiles.split("\\|");
                for(int i=0;i<msgArray.length;i++){
                    String[] tempStr = msgArray[i].split("\\-");
                    if(tempStr.length==2){
                        Log.e("tempstr","length of tempst: "+tempStr.length);
                        Log.e("tempstr",tempStr[0]+"   "+tempStr[1]);
                        matrixCursorObject.addRow(new String[]{tempStr[0], tempStr[1]});
                        cnn++;
                    }
                }
            }
        }

        //return cursor object
        flagStart=true;
        Log.e("cnn",""+cnn);
        return matrixCursorObject;

    }

    private Cursor getCursorOBJ(String selection){
        MatrixCursor matrixCursorObject = new MatrixCursor(tableColumns);
        String outPutMessage = "";
        File dir = getContext().getFilesDir();
        try{
            matrixCursorObject = new MatrixCursor(tableColumns);
            for(File file: dir.listFiles()){
                if(file.getName().equals(selection)) {
                    FileReader fileReader = new FileReader(file);
                    BufferedReader bufferedReader = new BufferedReader(fileReader);
                    StringBuffer stringBuffer = new StringBuffer();
                    String line = bufferedReader.readLine();
                    fileReader.close();
                    matrixCursorObject.addRow(new String[]{file.getName(), line});
                    Log.i("querying -- : ",file.getName()+ "   -   "+outPutMessage);
                }
            }
            return matrixCursorObject;
        }catch (IllegalArgumentException iae) {
            Log.e(TAG, "IllegalArgumentException in query ");
            iae.printStackTrace();
        } catch (NullPointerException npe) {
            Log.e(TAG, "NullPointerException in query ");
            npe.printStackTrace();
        } catch (Exception e) {
            Log.e(TAG, "Failed to return the cursor object!");
            e.printStackTrace();
        }
        return null;
    }

    private Cursor getFileFromAvd(Uri uri, String selection) throws CursorIndexOutOfBoundsException {
        ////query(buildObj1,null, inMessage[2], null, inMessage[3]);
        MatrixCursor matrixCursorObject = new MatrixCursor(tableColumns);
        String keyHashed = "";
        try {
            keyHashed = genHash(selection);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        //check whether this avd contains file
        if(successorNode==null || predecessorNode==null || keyHashed.compareTo(nodeId) ==0 || ((nodeId.compareTo(predecessorNode)==0) && (keyHashed.compareTo(predecessorNode)>0 || keyHashed.compareTo(predecessorNode)<0))
                || (predecessorNode.compareTo(nodeId)>0 && keyHashed.compareTo(predecessorNode)>0)
                || (predecessorNode.compareTo(nodeId)>0 && keyHashed.compareTo(nodeId)<0)
                || (keyHashed.compareTo(nodeId) <0 && keyHashed.compareTo(predecessorNode)>0)){

             return getCursorOBJ(selection);
        }
        //request successor for file
        else {
            String allFiles=null;
            allFiles = getFilesFromSuccessor("$-"+selection);
            String[] tempStr = allFiles.split("\\-");
            Log.e("single file","length of returned line object: "+tempStr.length);
            Log.e("single file","key is: "+tempStr[0]+"  value is: "+tempStr[1]);
            matrixCursorObject.addRow(new String[]{tempStr[0], tempStr[1]});
        }
        //return matrix cursor object
        return matrixCursorObject;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        MatrixCursor mxc = new MatrixCursor(tableColumns);
        Log.e("query",selection);
            if(selection.equals("*")){
                if(flagStart){
                    return getFilesfromAllServers(uri);
                }
                return mxc;
            } else if(selection.equals("@")) {
                return getAllFilesFromAvd(uri);
            } else {
                return getFileFromAvd(uri, selection);
            }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private void sendSuccPred(String str, int indx){
            int len = oneNode.size();
            String OpMsg="";
            if(indx ==0){
                if(len ==1){
                    OpMsg = "joinComplete"+"-"+dhtNodes.get(str)+ "-"+str + "." + dhtNodes.get(str)+ "-"+str+"."+dhtNodes.get(str);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, OpMsg);
                }else {
                    OpMsg = "joinComplete"+"-"+dhtNodes.get(str)+ "-"+ oneNode.get(1) + "." + dhtNodes.get(oneNode.get(1))+ "-" + oneNode.get(len-1)+"."+dhtNodes.get(oneNode.get(len-1));
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, OpMsg);
                }
            } else if(indx == len-1){
                OpMsg = "joinComplete"+"-"+dhtNodes.get(str)+ "-"+ oneNode.get(0) + "." + dhtNodes.get(oneNode.get(0))+ "-" + oneNode.get(len-2)+"."+dhtNodes.get(oneNode.get(len-2));
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, OpMsg);
            } else {
                OpMsg = "joinComplete"+"-"+dhtNodes.get(str)+ "-"+ oneNode.get(indx+1) + "." + dhtNodes.get(oneNode.get(indx+1))+ "-" + oneNode.get(indx-1)+"."+dhtNodes.get(oneNode.get(indx-1));
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, OpMsg);
            }
    }

    //telling predecessor that new node is your successor
    private void sendUpdateSucc(String str, int indx){
        int len = oneNode.size();
        String OpMsg;
        if(indx==0 && len >1){
            // msg, nodetoBeSent  success.Node
            OpMsg = "UpdSuc"+"-"+ dhtNodes.get(oneNode.get(len-1))+"-"+oneNode.get(indx) + "." + dhtNodes.get(oneNode.get(indx));
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, OpMsg);
        }else if(indx==0){
            OpMsg = "UpdSuc"+"-"+ dhtNodes.get(oneNode.get(0))+"-"+oneNode.get(indx) + "." + dhtNodes.get(oneNode.get(indx));
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, OpMsg);
        }else if(indx == len-1){
            OpMsg = "UpdSuc"+"-"+ dhtNodes.get(oneNode.get(len-2))+"-"+oneNode.get(indx) + "." + dhtNodes.get(oneNode.get(indx));
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, OpMsg);
        } else {
            OpMsg = "UpdSuc"+"-"+ dhtNodes.get(oneNode.get(indx-1))+"-"+oneNode.get(indx) + "." + dhtNodes.get(oneNode.get(indx));
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, OpMsg);
        }
    }

    private void sendUpdatePred(int indx){
        int len = oneNode.size();
        String OpMsg;
        if(indx==0 && len >1){
            // msg, nodetoBeSent  success.Node
            OpMsg = "UpdPred"+"-"+ dhtNodes.get(oneNode.get(1))+"-"+oneNode.get(indx) + "." + dhtNodes.get(oneNode.get(indx));
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, OpMsg);
        }else if(indx==0){
            OpMsg = "UpdPred"+"-"+ dhtNodes.get(oneNode.get(0))+"-"+oneNode.get(indx) + "." + dhtNodes.get(oneNode.get(indx));
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, OpMsg);
        }else if(indx == len-1){
            OpMsg = "UpdPred"+"-"+ dhtNodes.get(oneNode.get(0))+"-"+oneNode.get(indx) + "." + dhtNodes.get(oneNode.get(indx));
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, OpMsg);
        } else {
            OpMsg = "UpdPred"+"-"+ dhtNodes.get(oneNode.get(indx+1))+"-"+oneNode.get(indx) + "." + dhtNodes.get(oneNode.get(indx));
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, OpMsg);
        }
    }

    //server thread
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        private Uri.Builder uriObj;
        private Uri buildObj;
        private Uri buildObj1;
        private ContentValues contValObj;
        private ContentResolver contentResolverObj;

        private Uri makeUriObj(){
            uriObj = new Uri.Builder();
            uriObj.scheme("content");
            uriObj.authority(CONTENT_AUTHORITY);
            buildObj = uriObj.build();
            return buildObj;
        }

        private void UriFunction(String strReceived1){
            // keyMsg+"-"+valueMsg
            String[] strReceived = strReceived1.split("\\-");
            try{
                buildObj1 = makeUriObj();
                contValObj = new ContentValues();
                contValObj.put("value", strReceived[1]);
                contValObj.put("key", strReceived[0]);
                //System.out.println("In content provider    key : "+ Integer.toString(num).trim()+ "        "+ "value : "+ strReceived);
                num++;
                contentResolverObj =   getContext().getContentResolver();
                contentResolverObj.insert(buildObj1, contValObj);
            } catch (Exception e) {
                Log.e(TAG, "Exception in URI parsing ");
                e.printStackTrace();
            }
        }

        @Override
        protected Void doInBackground(ServerSocket... sockets){
            ServerSocket serverSocket = sockets[0];

            BufferedReader inReader=null;
            PrintStream printOut =null;
            Socket incomingSocket = null;
            try{
                while(true){
                    incomingSocket = serverSocket.accept();
                    inReader = new BufferedReader(new InputStreamReader(incomingSocket.getInputStream()));
                    printOut = new PrintStream(incomingSocket.getOutputStream());
                    String[] inMessage = inReader.readLine().split("\\-");
                    if(inMessage[0].equals("join")){
                        dhtNodes.put(inMessage[2],inMessage[3]);
                        oneNode.add(inMessage[2]);
                        Collections.sort(oneNode);
                        int indx = oneNode.indexOf(inMessage[2]);
                        // send succssor
                        sendSuccPred(inMessage[2], indx);
                        //send other two nodes for updating
                        sendUpdateSucc(inMessage[2],indx);
                        sendUpdatePred(indx);
                        Log.e("at 5554","List of nodes after join in chord is : "+oneNode.size());
                        Log.e("chord is: ","  "+Arrays.toString(oneNode.toArray()));
                        printOut.println("done!!");
                    } else if(inMessage[0].equals("joinComplete")){
                        successorNode = inMessage[2].split("\\.")[0];
                        dhtNodes.put(successorNode, inMessage[2].split("\\.")[1]);
                        predecessorNode = inMessage[3].split("\\.")[0];
                        dhtNodes.put(predecessorNode, inMessage[3].split("\\.")[1]);
                        Log.i("avd: "+inMessage[1],"successor: "+dhtNodes.get(successorNode)+"    predecessor: "+dhtNodes.get(predecessorNode));
                        printOut.println("done!!");
                    } else if(inMessage[0].equals("UpdSuc")){
                        Log.i("avd: "+inMessage[1],"successor: "+successorNode+"    predecessor: "+predecessorNode);
                        Log.i("avd: "+ inMessage[1], "successor being updates");
                        successorNode = inMessage[2].split("\\.")[0];
                        dhtNodes.put(successorNode, inMessage[2].split("\\.")[1]);
                        Log.i("avd: "+inMessage[1],"successor: "+dhtNodes.get(successorNode)+"    predecessor: "+dhtNodes.get(predecessorNode));
                        printOut.println("done!!");
                    } else if(inMessage[0].equals("UpdPred")){
                        Log.i("avd: "+inMessage[1],"successor: "+successorNode+"    predecessor: "+predecessorNode);
                        Log.i("avd: "+ inMessage[1], "predecessor being updates");
                        predecessorNode = inMessage[2].split("\\.")[0];
                        dhtNodes.put(predecessorNode, inMessage[2].split("\\.")[1]);
                        Log.i("avd: "+inMessage[1],"successor: "+dhtNodes.get(successorNode)+"    predecessor: "+dhtNodes.get(predecessorNode));
                        printOut.println("done!!");
                    } else if(inMessage[0].equals("insert")) {
                        Log.i("insert server", "on avd: "+inMessage[1] + " key: "+ inMessage[2]+  " hashed node: "+nodeId);
                        UriFunction(inMessage[2]+"-"+inMessage[3]);
                        printOut.println("done!!");
                    }  else if(inMessage[0].equals("*")) {
                        buildObj1 = makeUriObj();
                        Log.e("Server query", "Requst came on avd: "+portt+" type of query: "+inMessage[0]);
                        MatrixCursor mx = (MatrixCursor)  query(buildObj1,null, inMessage[0], null, null);
                        String line1="";
                        mx.moveToFirst();
                        while(!mx.isAfterLast()){
                          line1 += mx.getString(0)+"-"+mx.getString(1)+"|";
                          mx.moveToNext();
                          Log.e("at *","while loop");
                        }
                        printOut.println(line1);
                    } else if (inMessage[0].equals("$")) {
                        buildObj1 = makeUriObj();
                        Log.e("Server query", "Requst came on avd: "+portt+" type of query: "+inMessage[1]);
                        MatrixCursor mx = (MatrixCursor)  query(buildObj1,null, inMessage[1], null, null);
                        String line1="";
                        mx.moveToFirst();
                        line1 += mx.getString(0)+"-"+mx.getString(1);
                        printOut.println(line1);
                    } else if (inMessage[0].equals("#")) {// del single file
                        buildObj1 = makeUriObj();
                        delete(buildObj1, inMessage[1], null);
                        printOut.println("donee");
                    }
                }
            }catch(Exception e){
                System.out.println("Exception at Server side: " + e.toString());
                e.printStackTrace();
            }
            return null;
        }

        protected void onProgressUpdate(String...strings){

        }

    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try{
                String msgToSend[] = msgs[0].split("\\-");
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgToSend[1]));

                PrintStream printOut = null;
                BufferedReader inReader = null;
                printOut = new PrintStream(socket.getOutputStream());
                inReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                printOut.println(msgs[0]);
                printOut.flush();
                inReader.readLine();
                socket.close();
            }catch (SocketException se){
                se.printStackTrace();
            }
            catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
                e.printStackTrace();
            }
            return null;
        }
    }
}
