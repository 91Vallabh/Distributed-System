package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.EditText;
import android.widget.TextView;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.PriorityBlockingQueue;

import static java.lang.Double.parseDouble;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 * 
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {

    private static final String CONTENT_AUTHORITY = "edu.buffalo.cse.cse486586.groupmessenger2.provider";

    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final String[] REMOTE_PORT = {"11108", "11112", "11116","11120","11124"};
    static final int SERVER_PORT = 10000;
    int[] failNode = new int[5];
    public String mssgID = "";
    int msgNum =0;
    int msgServerNum =0;
    double fiSeq=0;
    public String portId = "";
    PriorityBlockingQueue<String[]> pQueue = new PriorityBlockingQueue<String[]>(100, new Comparator<String[]>(){
        public int compare(String[] a, String[] b) {
            if (Double.parseDouble(a[2])< Double.parseDouble(b[2]))
                return -1;
            else
                return 1;
        }
    });

    HashMap<String, Integer> hmapD = new HashMap<String, Integer>();                            // msgid       deliverable




    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);


        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        TextView localTextView = (TextView) findViewById(R.id.textView1);
        localTextView.append("\n");


        /*
         * Calculate the port number that this AVD listens on.
         */
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
//        System.out.println("this avd listens on : " + myPort);
        portId = myPort;
        //Log.e("cli is : " , myPort);

        /*
        Create a server socket as well as a thread (AsyncTask) that listens on the server
         */
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }

        /*
         Retrieve a pointer to the input box (EditText) defined in the layout
         */
        final EditText editText = (EditText) findViewById(R.id.editText1);


        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));


        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */
        findViewById(R.id.button4).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String msg = editText.getText().toString() + "\n";
                //System.out.println("message from avd: "+myPort+"  is: "+msg);
                editText.setText("");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
            }
        });
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }



    /*
     *taken from PA1
     */
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        private   int num=0;
        private Uri.Builder uriObj;
        private Uri buildObj;
        private ContentValues contValObj;
        private ContentResolver contentResolverObj;
        String portStr="";

        private void UriFunction(int num, String strReceived){

            try{
                uriObj = new Uri.Builder();
                uriObj.scheme("content");
                uriObj.authority(CONTENT_AUTHORITY);
                buildObj = uriObj.build();

                contValObj = new ContentValues();
                contValObj.put("value", strReceived);
                //contValObj.put("key", Integer.toString(num).trim());
                contValObj.put("key", num);
                //num++;

                contentResolverObj = getContentResolver();
                contentResolverObj.insert(buildObj, contValObj);
            } catch (Exception e) {
                Log.e(TAG, "Exception in URI parsing: " + e.toString());
            }
        }



        // input :  msgid   +     message
        protected String[] seqGenerator(String[] strings){
            String strId = strings[0].trim();
            String strReceived = strings[1].trim();
            String pId="";

            if(portId.equals("11108"))
                pId="1";
            if(portId.equals("11112"))
                pId="2";
            if(portId.equals("11116"))
                pId="3";
            if(portId.equals("11120"))
                pId="4";
            if(portId.equals("11124"))
                pId="5";

            msgServerNum = msgServerNum+1;
            String seq = Integer.toString(msgServerNum)+"."+pId;
            String[] re = {strId, strReceived, seq};
            return re;

        }


//        public void upFailedNode(){
//            int index2=-1;
//            if(portStr.equals("11108"))
//                index2=0;
//            if(portStr.equals("11112"))
//                index2=1;
//            if(portStr.equals("11116"))
//                index2=2;
//            if(portStr.equals("11120"))
//                index2=3;
//            if(portStr.equals("11124"))
//                index2=4;
//            Log.v("Server"," failed port is:"+portStr+" detected at : "+portId);
//            failNode[index2]=-1;
//        }

        //handling failed node
        public void removeFailedNode(){

            //removing failed node messages which are not deliverable

                List<String[]> l2 = new ArrayList<String[]>();
                while (pQueue.size() > 0) {
                    String[] fnode = pQueue.poll();
                    String[] analysis = fnode[0].split("-");
                    int index2 = -1;
                    if (analysis[0].equals("11108"))
                        index2 = 0;
                    if (analysis[0].equals("11112"))
                        index2 = 1;
                    if (analysis[0].equals("11116"))
                        index2 = 2;
                    if (analysis[0].equals("11120"))
                        index2 = 3;
                    if (analysis[0].equals("11124"))
                        index2 = 4;

                    if (failNode[index2] == -1 && hmapD.get(fnode[0]) == 0)
                        continue;
                    else
                        l2.add(fnode);
                }
                //adding back non failed node messages
                for (int i = 0; i < l2.size(); i++) {
                    pQueue.add(l2.get(i));
                }

        }

        public void removeFailedNodeException(){

            //removing failed node messages which are not deliverable

            List<String[]> l2 = new ArrayList<String[]>();
            while (pQueue.size() > 0) {
                String[] fnode = pQueue.poll();
                String[] analysis = fnode[0].split("-");
                int index2 = -1;
                if (analysis[0].equals("11108"))
                    index2 = 0;
                if (analysis[0].equals("11112"))
                    index2 = 1;
                if (analysis[0].equals("11116"))
                    index2 = 2;
                if (analysis[0].equals("11120"))
                    index2 = 3;
                if (analysis[0].equals("11124"))
                    index2 = 4;

                if (hmapD.get(fnode[0]) == 0)
                    continue;
                else
                    l2.add(fnode);
            }
            //adding back non failed node messages
            for (int i = 0; i < l2.size(); i++) {
                publishProgress(l2.get(i));
                Log.v("Server","After delivery of msg id: "+l2.get(i)[1]+"   queue size is: "+pQueue.size());
            }

        }


        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
//            int stt = serverSocket.getLocalPort();
            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */


            /*
            *           Initialiastions
             */
            BufferedReader inReader=null;
            Socket incomingSocket = null;
            ObjectInputStream inputStream=null;
            ObjectOutputStream outStream=null;
            DataInputStream din = null;


            try{
                while(true){

                    try {
                        serverSocket.setSoTimeout(4000);
                        incomingSocket = serverSocket.accept();

//                        portStr = String.valueOf(serverSocket.accept().getPort());
//                        din  = new DataInputStream(incomingSocket.getInputStream());
                        inReader = new BufferedReader(new InputStreamReader(incomingSocket.getInputStream()));
                        PrintStream out = new PrintStream(incomingSocket.getOutputStream());


                        //reading from client
//                        String inR= din.readLine();

                        String inR = inReader.readLine();

                        String[] inputs = inR.split("~");
                        Log.v("Server","Message received id: "+ inputs[0]+ "   message: "+inputs[1]);
                            //removing failed node messages which are not deliverable
                            removeFailedNode();

                            /*       */

                            if (hmapD.containsKey(inputs[0])) {

                                fiSeq = Double.parseDouble(inputs[2]);
//                        Log.v("Server:"," For msg id: "+inputs[0]+"  message: "+inputs[1]+"   final seq num received is: "+ inputs[2]);
//                        Log.v("Server:", "At present before updating lowest msg id in priority queue is : "+pQueue.peek()[0] + "  with priority as : "+ pQueue.peek()[2]);
                                msgServerNum = Math.max(msgServerNum, (int) fiSeq);
                                double tempSeq = 0;
                                List<String[]> l = new ArrayList<String[]>();
                                String[] rorder = null;


                                //updating sequnce number of the incoming message
                                while (pQueue.size() > 0) {
                                    rorder = pQueue.poll();
                                    if (rorder[0].equals(inputs[0])) {
                                        tempSeq = Double.parseDouble(inputs[2]);
                                        rorder[2] = Double.toString(tempSeq);
                                Log.v("Server:", "  at server msg id being updates is: "+ rorder[0]+"  message : "+ rorder[1]+"   final seq num is: "+rorder[2]);
                                        l.add(rorder);
                                        break;
                                    }
                                    l.add(rorder);
                                }
                                for (int k = 0; k < l.size(); k++) {


                                    pQueue.add(l.get(k));
                                }

                                hmapD.put(inputs[0], 1);
//                        Log.v("Server:", "At present after updating ,lowest msg id in priority queue is : "+pQueue.peek()[0] + "  with priority as : "+ pQueue.peek()[2] + " deliverable status: "+ hmapD.get(inputs[0]));


                                while ((pQueue.size() > 0) && hmapD.get(pQueue.peek()[0]) == 1) {
//                            Log.v("Server:", " Avd at port : "+portId+"  priority queue size is before delivering  "+pQueue.size());
                                    String[] sttr = pQueue.poll();
                            Log.v("Server: ", "  msg being delivered is : "+sttr[0]+ "   "+ sttr[1]+"  with seq as : "+ sttr[2]);
//                            if(pQueue.size()>0)
//                                Log.v("Server: ", "  after delivery next smallest msg id is : "+pQueue.peek()[0]+ "   "+ pQueue.peek()[1]+"  with seq as : "+ pQueue.peek()[2]);
                                    publishProgress(sttr);
                                    Log.v("Server","After delivery of msg id: "+sttr[1]+"   queue size is: "+pQueue.size());
                                }


                                //
                                //write back to client
                                out.println("done");
                                out.flush();
//                                din.close();
                                out.close();
                            } else {
                                String[] re = seqGenerator(inputs);


                                //              writing back to server

                                // msgid      message      sequence num
                                //adding message to priority queue
                                pQueue.add(re);

                                hmapD.put(re[0], 0);

                                String ore = re[0] + "~" + re[1] + "~" + re[2];
                                out.println(ore);
//                        Log.v("Server: ", "Server : "+portId+"  is sending for msg id : "+re[0]+"    message is: "+re[1]+"   sequnce num as : "+re[2]);
                            }

                    }catch(SocketTimeoutException se){
                        Log.e("Server","timeout Exception at server side  ");
                        se.printStackTrace();
                        removeFailedNodeException();
                        continue;
                    }catch(StreamCorruptedException se){
                        Log.e("Server","stream corrupted Exception at server side  ");
                        se.printStackTrace();
                        removeFailedNodeException();
                        continue;
                    } catch(EOFException se){
                        Log.e("Server","EOF Exception at server side  ");
                        se.printStackTrace();
                        removeFailedNodeException();
                        continue;
                    } catch(FileNotFoundException se) {
                        Log.e("Server","FNF Exception at server side  ");
                        se.printStackTrace();
                        removeFailedNodeException();
                        continue;
                    } catch(IOException se){
                        Log.e("Server","IO Exception at server side  ");
                        se.printStackTrace();
                        removeFailedNodeException();
                        continue;
                    }
                }
            }catch(Exception e){
                Log.e("Server","Exception at server side  ");
                e.printStackTrace();
            }

            return null;
        }

        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String strReceived = strings[1].trim();
            String msgid = strings[0].trim();
            TextView tv = (TextView) findViewById(R.id.textView1);
            tv.append(num + "   " + strReceived + "\n");
            TextView localTextView = (TextView) findViewById(R.id.textView1);
            localTextView.append("\n");
            UriFunction(num,strReceived);
            num++;
            return;
        }
    }


    /*
     *taken from PA1
     */
    private class ClientTask extends AsyncTask<String, Void, Void> {

        //handling failures


        @Override
        protected Void doInBackground(String... msgs) {
            int failIndex=0;


            try {
                Socket socket=null;
                msgNum = msgNum+1;
                Double[] pseq = new Double[5];
                int index=0;
                String[] inputs = null;

                for(String port:REMOTE_PORT) {
                    if(port.equals("11108"))
                        failIndex=0;
                    if(port.equals("11112"))
                        failIndex=1;
                    if(port.equals("11116"))
                        failIndex=2;
                    if(port.equals("11120"))
                        failIndex=3;
                    if(port.equals("11124"))
                        failIndex=4;
                    try {

                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(port));
                        socket.setSoTimeout(3000);

                        String msgToSend = msgs[0];
                        /*
                         *creating message id
                         */
                        mssgID = portId +"-"+ Integer.toString(msgNum);
                        String opMsg = mssgID + "~" + msgToSend;
                        /*
                         *Initialisations
                         */
                        BufferedReader inReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
//                        DataInputStream din = new DataInputStream(socket.getInputStream());
                        PrintStream out = new PrintStream(socket.getOutputStream());


//                                  SEDNING MESSAGE
//                        Log.v("Client","before sending");
                        out.println(opMsg);

                        String opMsg12[] = opMsg.split("~");
                        Log.v("From client", "Client sending new nessage, msg id is: "+opMsg12[0]+"  message is: "+opMsg12[1]);




                        //          RECEIVING SEQUENCE NUMBER FROM SERVER
                        //     msgID       message           sequence num

                        Log.v("Client","waiting for ack"+ "  from port "+ port);
//                        String inStr = din.readUTF();
                        String inStr = inReader.readLine();
//                        Log.v("To Client", "Client reived seq num from port id: "+ port+"  msgid: "+ inputs[0]+ "   mesage is : "+ inputs[1]+"  seq num receieved is: "+inputs[2]);
                        Log.v("Client","ack received");
                        inputs = inStr.split("~");
                        pseq[index++] = Double.parseDouble(inputs[2]);
                        inReader.close();
//                        din.close();
                        out.close();
                        socket.close();
                    }catch(NullPointerException se){
                        Log.e("Client","Null pointer Exception at client side  "+ "   port:  "+port);
                        se.printStackTrace();
                        failNode[failIndex] = -1;
                        pseq[index++] = Double.valueOf(0);
                        socket.close();
                        continue;
                    }catch(SocketTimeoutException se){
                        Log.e("Client","timeout Exception at client side  "+ "   port:  "+port);
                        se.printStackTrace();
                        failNode[failIndex] = -1;
                        pseq[index++] = Double.valueOf(0);
                        socket.close();
                        continue;
                    }catch(StreamCorruptedException se){
                        Log.e("Client","Stream Corrupted Exception at client side  "+ "   port:  "+port);
                        se.printStackTrace();
                        failNode[failIndex] = -1;
                        pseq[index++] = Double.valueOf(0);
                        socket.close();
                        continue;
                    } catch(EOFException se){
                        Log.e("Client","EOF Exception at client side  "+ "   port:  "+port);
                        se.printStackTrace();
                        failNode[failIndex] = -1;
                        pseq[index++] = Double.valueOf(0);
                        socket.close();
                        continue;
                    }catch(FileNotFoundException fe){
                        Log.e("Client","FNF Exception at client side  "+ "   port:  "+port);
                        fe.printStackTrace();
                        failNode[failIndex] = -1;
                        pseq[index++] = Double.valueOf(0);
                        socket.close();
                        continue;
                    } catch(IOException se){
                        Log.e("Client","IO Exception at client side  "+ "   port:  "+port);
                        se.printStackTrace();
                        failNode[failIndex] = -1;
                        pseq[index++] = Double.valueOf(0);
                        socket.close();
                        continue;
                    }
                }



                /*           remulticasting final sequnce number to all avds                    */


                //highest seqnumber
                Double max = pseq[0];
                for(double d : pseq){
                    if(max<d)
                        max=d;
                }
                inputs[2] = Double.toString(max);
//                Log.v("AT Client: ", "Final seq for message id: "+inputs[0]+ "  message is : "+inputs[1]+"   final sequnce num is : "+inputs[2]);

                for(String port:REMOTE_PORT) {
                    if(port.equals("11108"))
                        failIndex=0;
                    if(port.equals("11112"))
                        failIndex=1;
                    if(port.equals("11116"))
                        failIndex=2;
                    if(port.equals("11120"))
                        failIndex=3;
                    if(port.equals("11124"))
                        failIndex=4;
                    try {
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(port));
                        socket.setSoTimeout(3000);
                        BufferedReader inReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
//                        DataInputStream din = new DataInputStream(socket.getInputStream());
                        PrintStream out = new PrintStream(socket.getOutputStream());

                        //          SEDNING final priority
                        String opMsg = inputs[0] + "~" + inputs[1] + "~" + inputs[2];
                        out.println(opMsg);
//                        Log.v("Final client", "Final sequnce num from clinet for message id: "+inputs[0]+"  message: "+inputs[1]+"   seq num is: "+inputs[2]);
//                        din.close();
//                        din.readUTF();
                        inReader.readLine();
                        inReader.close();
                        out.close();
                        socket.close();
                    }catch(SocketTimeoutException se){
                        Log.e("Client","2nd time timeout Exception at client side  "+ "   port:  "+port);
                        se.printStackTrace();
                        failNode[failIndex] = -1;
                        socket.close();
                        continue;
                    }catch(StreamCorruptedException se){
                        Log.e("Client","2nd time Stream Corrupted Exception at client side  "+ "   port:  "+port);
                        se.printStackTrace();
                        failNode[failIndex] = -1;
                        socket.close();
                        continue;
                    } catch(EOFException se){
                        Log.e("Client","2nd time EOF Exception at client side  "+ "   port:  "+port);
                        se.printStackTrace();
                        failNode[failIndex] = -1;
                        socket.close();
                        continue;
                    }catch(FileNotFoundException fe){
                        Log.e("Client","2nd time FNF Exception at client side  "+ "   port:  "+port);
                        fe.printStackTrace();
                        failNode[failIndex] = -1;
                        socket.close();
                        continue;
                    } catch(IOException se){
                        Log.e("Client","2nd time IO Exception at client side  "+ "   port:  "+port);
                        se.printStackTrace();
                        failNode[failIndex] = -1;
                        socket.close();
                        continue;
                    }
                }



//                Log.v("Client thread end","all message delivered");
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
                e.printStackTrace();
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
                e.printStackTrace();
            }
            catch(Exception e){
                Log.e("Client","Exception at client side  ");
                e.printStackTrace();
            }

            return null;
        }
    }

}


/*
 *https://developer.android.com/reference/android/database/Cursor.html
 * https://developer.android.com/reference/android/content/ContentUris.html#withAppendedId(android.net.Uri,%20long)
 * https://developer.android.com/reference/android/database/MatrixCursor.html#addRow(java.lang.Object[])
 * https://developer.android.com/guide/topics/providers/content-provider-creating#Query
 *
 *
 *
 */