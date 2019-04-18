package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
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
import java.util.Collection;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

public class SimpleDynamoProvider extends ContentProvider {

	private static final String CONTENT_AUTHORITY = "edu.buffalo.cse.cse486586.simpledynamo.provider";
	static final int SERVER_PORT = 10000;
	String myPortt;
	String nodeId;
	HashMap<String, String> dhtNodes = new HashMap<String, String>();// hashed index  -  port number
	static final String[] REMOTE_PORT = {"11108", "11112", "11116","11120","11124"};
	List<String> preferenceList = new ArrayList<String>();
	List<String> nodeList = new ArrayList<String>();
	Set<String> msgMap = new HashSet<String>();
	private String[] tableColumns = {"key","value"};




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
		this.myPortt = myPort;


		try {
			this.nodeId = genHash(Integer.toString(Integer.parseInt(myPort) / 2));// 5554
			Log.i("node", this.nodeId);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		//create a list of all nodes their hashed value and their port
		initializeNodeInfo();
		Log.i("initializeNodeInfo","Chord ring size is: "+ this.nodeList.size());
		Log.i("initializeNodeInfo","Chord ring is: "+ Arrays.toString(this.nodeList.toArray()));
		//creates a preference list
		createPreferenceList();
		Log.i("createPreferenceList","preference list size is: "+ this.preferenceList.size());
		Log.i("createPreferenceList","preference list is: "+ Arrays.toString(this.preferenceList.toArray()));

		//create a server for this avd
		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
			e.printStackTrace();
		}
		return true;
	}

	private void createPreferenceList(){
		int index = this.nodeList.indexOf(this.nodeId);
		// 0 1 2 3 4
		Log.i("createPreferenceList","node is: "+this.dhtNodes.get(this.nodeId)+"  index in chord is: "+index);
		if (index < (this.nodeList.size()-2)) {
			this.preferenceList.add(this.nodeList.get(this.nodeList.indexOf(this.nodeId)+1));
			this.preferenceList.add(this.nodeList.get(this.nodeList.indexOf(this.nodeId)+2));
		} else if (index < (nodeList.size()-1)) {
			this.preferenceList.add(this.nodeList.get(this.nodeList.indexOf(this.nodeId)+1));
			this.preferenceList.add(this.nodeList.get(0));
		} else if (index == (nodeList.size()-1)){
			this.preferenceList.add(this.nodeList.get(0));
			this.preferenceList.add(this.nodeList.get(1));
		}
	}

	private void initializeNodeInfo(){
		try {
			for (String port : REMOTE_PORT) {
				String temp = genHash(Integer.toString(Integer.parseInt(port) / 2));
				dhtNodes.put(temp,port);
				nodeList.add(temp);
				Log.i("initializeNodeInfo","avd: "+port+"  hashed value: "+ temp);
			}
			Collections.sort(nodeList);
		}catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}


	//a function which takes a key and returns which avd it belongs to
	private String targetNode(String keyy){
		String keyHashed = keyy;
//		try{
//			keyHashed = genHash(keyy);
//		} catch (NoSuchAlgorithmException e) {
//			e.printStackTrace();
//		}
		String node = null;
		for(String temp:this.nodeList){
			if(keyHashed.compareTo(temp) <=0){
				node=temp;
				break;
			}
		}
		if(node == null){
			node = this.nodeList.get(0);
		}
		Log.i("targetNode"," key to be found is: "+keyy+" target node found is: "+this.dhtNodes.get(node) + " target node hashed is: "+node);
		return this.dhtNodes.get(node);
	}


	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	private Uri makeUriObj(){
		Uri.Builder uriObj;
		Uri buildObj;
		uriObj = new Uri.Builder();
		uriObj.scheme("content");
		uriObj.authority(CONTENT_AUTHORITY);
		buildObj = uriObj.build();
		return buildObj;
	}

	private boolean isContainedInAvd(String keym){
		return this.msgMap.contains(keym);
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
		return getCursorOBJ(selection);
	}

	private void inserMsg (String keyMsg, String line11, String keyHashed) {
		FileOutputStream outputStream =   null;
		try {
			outputStream = getContext().openFileOutput(keyMsg, Context.MODE_PRIVATE);
			outputStream.write(line11.getBytes());
			outputStream.close();
			this.msgMap.add(keyMsg);
			Log.i("key Insert", "  port: " + myPortt + "  inserted key : " + keyMsg + "   key hashed value  " + keyHashed + "  value: " + line11);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void sendToPreferences(String keyMsg, String line11){
		String OpMsg = "insertPref"+"-"+ dhtNodes.get(preferenceList.get(0))+"-"+keyMsg+"-"+line11;
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, OpMsg);
		Log.i("insert", " insert going from: "+myPortt+" sending to preference node: "+dhtNodes.get(preferenceList.get(0))+ " key is: "+ keyMsg+"  msg is: "+ line11);
		OpMsg = "insertPref"+"-"+ dhtNodes.get(preferenceList.get(1))+"-"+keyMsg+"-"+line11;
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, OpMsg);
		Log.i("insert", " insert going from: "+myPortt+" sending to preference node: "+dhtNodes.get(preferenceList.get(1))+ " key is: "+ keyMsg+"  msg is: "+ line11);
	}


	@Override
	public Uri insert(Uri uri, ContentValues values) {
		return null;
	}




//	@Override
//	public Uri insert(Uri uri, ContentValues values) {
//		// TODO Auto-generated method stub
//		// insert request came at wrong avd
//		// insert request came at right avd
//		// inser came from another avd
//
//		String keyMsg;
//		String valueMsg[];
//		int version=0;
//		String values1=null;
//		String tempcheck=null;
//		keyMsg =  values.get("key").toString();
//		valueMsg  =  values.get("value").toString().split("\\.");
//		try {
//			version = Integer.parseInt(valueMsg[1]);
//			tempcheck = valueMsg[3];
//			Log.i("insert", "for msg key : "+ keyMsg+" version is: "+version);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//
//		String keyHashed="";
//		try {
//			keyHashed = genHash(keyMsg);
//		} catch (NoSuchAlgorithmException e) {
//			e.printStackTrace();
//		}
//		Log.i("in insert: ", "insert key: "+ keyMsg+ " hashed kymsg: "+keyHashed);
//
//
//		String trgtNode = targetNode(keyHashed);
//		if(trgtNode.equals(this.myPortt)) {
//			Log.i("insert", " key :"+keyMsg+"  is to ber stored at this avd: "+this.myPortt);
//			// insert here and at 2 successor node
//			if(isContainedInAvd(keyMsg)) {
//				Log.i("insert"," msg is already contained in this avd");
//				MatrixCursor mx = (MatrixCursor) getFileFromAvd(makeUriObj(),keyMsg);
//				mx.moveToFirst();
//				//compare the version number
//				int ver = Integer.parseInt(mx.getString(1).split("\\.")[1]) + 1;
//				try{
//					ver = Math.max(ver, version);
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
//				String mssg = mx.getString(1).split("\\.")[0];
//				String send = mx.getString(1).split("\\.")[2];
//				String line11 = mssg + "."+Integer.toString(ver) + "."+send;
//				inserMsg(keyMsg, line11, keyHashed);
//				Log.i("insert"," key "+keyMsg+" is being stored with version :"+ ver);
//				if(send.equals(this.myPortt)){
//					sendToPreferences(keyMsg,line11);
//				}
//			} else {
//				//create version number
//				//insert it
//				//check whether to send forwrd
//				Log.i("insert"," this avd : "+myPortt+"  does not contain this msg: "+keyMsg);
//				String send=this.myPortt;
////				if(valueMsg.length>1){
////					send = valueMsg[2];
////					Log.i("insert", " send is updated to :"+send+ " my port is; "+this.myPortt);
////				}
//				String line11 = valueMsg[0] + "."+Integer.toString(1)+"." + send;
//				Log.i("line1111",line11);
//				inserMsg(keyMsg, line11, keyHashed);
//				sendToPreferences(keyMsg,line11);
////				if(send.equals(this.myPortt)){
////					sendToPreferences(keyMsg,line11);
////				}
//			}
//		} else {//wrong avd
//			// append true to sendToPreferenceNode
//			// send to trgtNode
//			if(tempcheck !=null){//from coordinator
//				if(isContainedInAvd(keyMsg)) {//updating msg
//					Log.i("insert"," msg from coordinator and is already contained in this avd");
//					MatrixCursor mx = (MatrixCursor) getFileFromAvd(makeUriObj(),keyMsg);
//					mx.moveToFirst();
//					//compare the version number
//					int ver = Integer.parseInt(mx.getString(1).split("\\.")[1]) + 1;
//					try{
//						ver = Math.max(ver, version);
//					} catch (Exception e) {
//						e.printStackTrace();
//					}
//					String mssg = mx.getString(1).split("\\.")[0];
//					String send = mx.getString(1).split("\\.")[2];
//					String line11 = mssg + "."+Integer.toString(ver) + "."+send;
//					inserMsg(keyMsg, line11, keyHashed);
//					Log.i("insert"," key "+keyMsg+" is being stored with version :"+ ver);
//				}else {//from coordinator but msg coming for first time
//					inserMsg(keyMsg, (values.get("value").toString()), keyHashed);
//					Log.i("insert pref", " came from coordinator key is: " + keyMsg);
//				}
//			} else {
//				String OpMsg = "insertWAVD" + "-" + trgtNode + "-" + keyMsg + "-" + valueMsg[0];
//				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, OpMsg);
//				Log.i("insert", " insert came at wrong avd: " + this.myPortt + " sending to: " + trgtNode + " key is: " + keyMsg + "  msg is :" + valueMsg[0]);
//			}
//		}
//
//
//		return null;
//	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
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

	//server thread
	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		private Uri buildObj1;
		private ContentValues contValObj;
		private ContentResolver contentResolverObj;

		private void UriFunction(String strReceived1){
			// keyMsg+"-"+valueMsg
			String[] strReceived = strReceived1.split("\\-");
			try{
				buildObj1 = makeUriObj();
				contValObj = new ContentValues();
				contValObj.put("value", strReceived[1]);
				contValObj.put("key", strReceived[0]);
				//System.out.println("In content provider    key : "+ Integer.toString(num).trim()+ "        "+ "value : "+ strReceived);
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
					//String OpMsg = "insertPref"+"-"+ dhtNodes.get(preferenceList.get(0))+"-"+keyMsg+"-"+line11;
					if(inMessage[0].equals("insertWAVD") || inMessage[0].equals("insertPref")) {
						Log.i("server", "Insert on avd: "+inMessage[1] + " key: "+ inMessage[2]+  " msg "+inMessage[3]);
						UriFunction(inMessage[2]+"-"+inMessage[3]);
						printOut.println("done!!");
					}
				}
			} catch(Exception e){
				System.out.println("Exception at Server side");
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
				Log.i("client:","from avd: "+myPortt+" msg is being sent to : "+msgToSend[1]+" type of msg is: "+msgToSend[0]);
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
