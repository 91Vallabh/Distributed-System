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
import java.net.SocketTimeoutException;
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
import java.util.Map;
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
	Map<String, String> dhtNodes = new HashMap<String, String>();// hashed index  -  port number
	static final String[] REMOTE_PORT = {"11108", "11112", "11116","11120","11124"};
	List<String> preferenceList = new ArrayList<String>();
	List<String> nodeList = new ArrayList<String>();
	Set<String> msgMap = new HashSet<String>();
	private String[] tableColumns = {"key","value"};
	boolean flagStart=true;
	HashMap<String, String> keyToNode = new HashMap<String, String>();
	List<String> repList = new ArrayList<String>();
	HashMap<String, List<String>> NodeToKey = new HashMap<String, List<String>>();




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
		//Log.i("initializeNodeInfo","Chord ring size is: "+ this.nodeList.size());
		Log.i("initializeNodeInfo","Chord ring is: "+ Arrays.toString(this.nodeList.toArray()));
		//creates a preference list
		createPreferenceList();
		createReplicaList();
		//Log.i("createPreferenceList","preference list size is: "+ this.preferenceList.size());
		//Log.i("createPreferenceList","preference list is: "+ Arrays.toString(this.preferenceList.toArray()));

		//create a server for this avd
		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
			e.printStackTrace();
		}

		deleteAllFiles();
		//Log.i("on create","this avd port is: "+this.myPortt);
		return true;
	}

	private void deleteAllFiles(){
		MatrixCursor matrixCursorObject = new MatrixCursor(tableColumns);
		File dir = getContext().getFilesDir();
		try{
			matrixCursorObject = new MatrixCursor(tableColumns);
			for(File file: dir.listFiles()){
				FileReader fileReader = new FileReader(file);
				file.delete();
			}
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
	}

	//0 1 2 3 4

	private void createReplicaList(){
		int index = this.nodeList.indexOf(this.nodeId);
		if (index > (this.nodeList.size()-4)) {
			this.repList.add(this.nodeList.get(this.nodeList.indexOf(this.nodeId)-1));
			this.repList.add(this.nodeList.get(this.nodeList.indexOf(this.nodeId)-2));
		} else if (index > 0) {
			this.repList.add(this.nodeList.get(this.nodeList.indexOf(this.nodeId)-1));
			this.repList.add(this.nodeList.get(4));
		} else if (index == 0){
			this.repList.add(this.nodeList.get(4));
			this.repList.add(this.nodeList.get(3));
		}
	}

	private void createPreferenceList(){
		int index = this.nodeList.indexOf(this.nodeId);
		// 0 1 2 3 4
		//Log.i("createPreferenceList","node is: "+this.dhtNodes.get(this.nodeId)+"  index in chord is: "+index);
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
				//Log.i("initializeNodeInfo","avd: "+port+"  hashed value: "+ temp);
			}
			Collections.sort(nodeList);
		}catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}

	//a function which takes a key and returns which avd it belongs to
	private String targetNode(String keyy){
		String keyHashed = "";
		try {
			keyHashed = genHash(keyy);
			//Log.i("getFileFromAvd","key hashed key is: "+keyy);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

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
		//Log.i("targetNode"," key to be found is: "+keyy+" target node found is: "+this.dhtNodes.get(node) + " target node hashed is: "+node);
		return this.dhtNodes.get(node);
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

	private void inserMsg (String keyMsg1, String line111, String keyHashed2) {
		FileOutputStream outputStream =   null;
		try {
			outputStream = getContext().openFileOutput(keyMsg1, Context.MODE_PRIVATE);
			outputStream.write(line111.getBytes());
			outputStream.close();
//			this.keyToNode.put(keyMsg1)
			this.msgMap.add(keyMsg1);
			Log.i("Inserted", "  port: " + myPortt + "  inserted key : " + keyMsg1 + "   key hashed value  " + keyHashed2 + "  value: " + line111);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void sendToPreferences(String keyMsg1, String line111){
		String OpMsg = "insertPref"+"-"+ this.dhtNodes.get(this.preferenceList.get(0))+"-"+keyMsg1+"-"+line111;
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, OpMsg);
		Log.i("insert", " insert going from: "+this.myPortt+" sending to preference node: "+this.dhtNodes.get(this.preferenceList.get(0))+ " key is: "+ keyMsg1+"  msg is: "+ line111);
		OpMsg = "insertPref"+"-"+ this.dhtNodes.get(this.preferenceList.get(1))+"-"+keyMsg1+"-"+line111;
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, OpMsg);
		Log.i("insert", " insert going from: "+this.myPortt+" sending to preference node: "+this.dhtNodes.get(this.preferenceList.get(1))+ " key is: "+ keyMsg1+"  msg is: "+ line111);
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub

		String keyMsg;
		String valueMsg[];

		keyMsg =  values.get("key").toString();
		valueMsg  =  values.get("value").toString().split("\\.");

		String keyHashed="";
		try {
			keyHashed = genHash(keyMsg);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		//Log.i("insert: ", "insert key: "+ keyMsg+ " hashed kymsg: "+keyHashed);

		String trgtNode = targetNode(keyMsg);
		Log.i("insert", "for key: "+keyMsg+"  target node is: "+trgtNode+"  this node is: "+this.myPortt);

		if(trgtNode.equals(this.myPortt)) {
			Log.i("insert", " key :"+keyMsg+"  is to be stored at this avd: "+this.myPortt);
			String send=this.myPortt;
			String line11="";
			line11 = valueMsg[0] + "." + send;
			//Log.i("coordinator","insert at coordinator: "+line11);
			inserMsg(keyMsg, valueMsg[0], keyHashed);
			storeNodetoKeyMapping(keyMsg);
			this.keyToNode.put(keyMsg, this.myPortt);
			sendToPreferences(keyMsg,line11);
		} else {//wrong avd
			String OpMsg = "insertWAVD" + "-" + trgtNode + "-" + keyMsg + "-" + (valueMsg[0]+ "." +trgtNode);
				new ClientTask1().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, OpMsg);
				Log.i("insert", " insert came at wrong avd: " + this.myPortt + " sending to: " + trgtNode + " key is: " + keyMsg + "  msg is :" + (valueMsg[0]+ "." +trgtNode));
		}
		return null;
	}

	private void storeNodetoKeyMapping(String keyMsg1122){
		if(this.keyToNode.containsKey(keyMsg1122)){//means this msg is coming second time for insert
			return;
		} else {
			if(this.NodeToKey.containsKey(this.myPortt)){
				List<String> temp = this.NodeToKey.get(this.myPortt);
				temp.add(keyMsg1122);
				this.NodeToKey.put(this.myPortt,temp);
			} else {
				List<String> temp = new ArrayList<String>();
				temp.add(keyMsg1122);
				this.NodeToKey.put(this.myPortt,temp);
			}
		}
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
					String line = bufferedReader.readLine().split("\\.")[0];
					fileReader.close();
					matrixCursorObject.addRow(new String[]{file.getName(), line});
					Log.i("getCursorOBJ",file.getName()+ "   -   "+outPutMessage);
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

	private String getFilesFromSuccessor(String str, String targetNode){
		String line=null;
		String nextPort="";
		String nextPort2="";
		Socket socket=null;
//		String temp = genHash(Integer.toString(Integer.parseInt(msgToSend[1]) / 2)); //targetNode
		String tempp=null;
		try {
			tempp= genHash(Integer.toString(Integer.parseInt(targetNode)/2));
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		Log.i("getFilesFromSuccessor"," target node is"+targetNode+ "  hashed vallue is: "+ tempp );
		int indexS=0;
		for(int i=0;i<nodeList.size();i++){
			if(tempp.equals(nodeList.get(i))){
				indexS=i;
				break;
			}
		}
		Log.i("getFilesFromSuccessor:"," index found is: "+indexS);
		if(indexS==(nodeList.size()-1)) {
			nextPort = dhtNodes.get(nodeList.get(0));
			nextPort2 = dhtNodes.get(nodeList.get(1));
		}
		else if(indexS==(nodeList.size()-2)) {
			nextPort = dhtNodes.get(nodeList.get(indexS + 1));
			nextPort2 = dhtNodes.get(nodeList.get(0));
		} else {
			nextPort = dhtNodes.get(nodeList.get(indexS + 1));
			nextPort2 = dhtNodes.get(nodeList.get(indexS + 2));
		}
		String[] ports={targetNode,nextPort,nextPort2};
		try{

			for(String port:ports) {
				try {

					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(port));
					socket.setSoTimeout(1000);
					PrintStream printOut = null;
					BufferedReader inReader = null;
					printOut = new PrintStream(socket.getOutputStream());
					inReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					Log.e("to succ", "response from avd: " + this.myPortt + " type of query: " + str.split("\\-")[0]);
					printOut.println(str);
					printOut.flush();
					line = inReader.readLine();
					Log.e("from succ", "response came on avd: " + this.myPortt + " type of query: " + str.split("\\-")[0] + "  response is: " + line);
					socket.close();
					break;
				} catch(SocketTimeoutException e){
					socket.close();
					e.printStackTrace();
					continue;
				} catch(NullPointerException e){
					socket.close();
					e.printStackTrace();
					continue;
				} catch (UnknownHostException e) {
					socket.close();
					e.printStackTrace();
					continue;
				} catch(IOException e){
					socket.close();
					e.printStackTrace();
					continue;
				} catch(Exception e){
					socket.close();
					e.printStackTrace();
					continue;
				}
			}
		} catch (Exception e){
			e.printStackTrace();
		}
		return line;
	}

	private String getFilesFromSuccessor1(String str, String targetNode){
				Socket socket=null;
				String line=null;
				try {
					try {
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(targetNode));
						socket.setSoTimeout(1000);
						PrintStream printOut = null;
						BufferedReader inReader = null;
						printOut = new PrintStream(socket.getOutputStream());
						inReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
						Log.e("to succ", "response from avd: " + this.myPortt + " type of query: " + str.split("\\-")[0]);
						printOut.println(str);
						printOut.flush();
						line = inReader.readLine();
						Log.e("from succ", "response came on avd: " + this.myPortt + " type of query: " + str.split("\\-")[0] + "  response is: " + line);
						socket.close();
					} catch (SocketTimeoutException e) {
						socket.close();
						e.printStackTrace();
					} catch (NullPointerException e) {
						socket.close();
						e.printStackTrace();
					} catch (UnknownHostException e) {
						socket.close();
						e.printStackTrace();
					} catch (IOException e) {
						socket.close();
						e.printStackTrace();
					} catch (Exception e) {
						socket.close();
						e.printStackTrace();
					}
				} catch (Exception e){
					e.printStackTrace();
				}

		return line;
	}

	private Cursor getFileFromAvd(Uri uri, String selection) throws CursorIndexOutOfBoundsException {
		MatrixCursor matrixCursorObject = new MatrixCursor(tableColumns);
		String trgtNode = targetNode(selection);


		if(trgtNode.equals(this.myPortt)){//get file from this avd
			Log.i("getFileFromAvd","file mapped to this avd");
			return getCursorOBJ(selection);
		} else {//forward request
			Log.i("getFileFromAvd","file not mapped to this avd forwarding request to this avd: "+ trgtNode);
			String allFiles=null;
			allFiles = getFilesFromSuccessor("$-"+selection, trgtNode);
			if(allFiles!=null) {
				String[] tempStr = allFiles.split("\\-");
				Log.e("getFileFromAvd", "length of returned line object: " + tempStr.length);
				Log.e("getFileFromAvd", "key is: " + tempStr[0] + "  value is: " + tempStr[1]);
				matrixCursorObject.addRow(new String[]{tempStr[0], (tempStr[1].split("\\.")[0])});
			}
		}


//		String key1="";
//		String value1="";
//		matrixCursorObject.moveToFirst();
//		key1 = matrixCursorObject.getString(0);
//		value1 = matrixCursorObject.getString(1).split("\\.")[0];



		return matrixCursorObject;
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
				String line = bufferedReader.readLine().split("\\.")[0];
				outPutMessage = line;
				fileReader.close();
				matrixCursorObject.addRow(new String[]{file.getName(), line});
				//cnn++;
				Log.i("querying -- : ",file.getName()+ "   -   "+line);
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

	private Cursor getFilesfromAllServers(Uri uri){
		//get all messages from this avd
		MatrixCursor matrixCursorObject = new MatrixCursor(tableColumns);
//		matrixCursorObject =  (MatrixCursor) getAllFilesFromAvd(uri);
		//get messages from successor avd

		String allFiles=null;
		String[] msgArray=null;
		for(String port:REMOTE_PORT){
			allFiles = getFilesFromSuccessor1("*-@", port);
			if(!allFiles.equals(null)) {
				msgArray = allFiles.split("\\|");
				for (int i = 0; i < msgArray.length; i++) {
					String[] tempStr = msgArray[i].split("\\-");
					if (tempStr.length == 2) {
						matrixCursorObject.addRow(new String[]{tempStr[0], (tempStr[1].split("\\.")[0])});
					}
				}
			}
		}
		return matrixCursorObject;

	}


	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		MatrixCursor mxc = new MatrixCursor(tableColumns);
		Log.e("query",selection);
		if (selection.equals("*")){
			return getFilesfromAllServers(uri);
		} else if(selection.equals("@")) {
			return getAllFilesFromAvd(uri);
		} else {
			return getFileFromAvd(uri, selection);
		}
	}

	private void delFilesfromAllServers(){
		delAllFilesFromAvd();
		for(String port: REMOTE_PORT){
			delFilesFromSuccessor1("del*",port);
		}
	}

	private void delAllFilesFromAvd(){
		File dir = getContext().getFilesDir();
		try{
			for(File file: dir.listFiles()){
				this.keyToNode.remove(file.getName());
				file.delete();
			}
			this.NodeToKey.remove(this.myPortt);
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
	}

	private void delFilesFromSuccessor(String str, String targetNode){
		Socket socket=null;
		String line=null;
		String nextPort="";
		String nextPort2="";
//		String temp = genHash(Integer.toString(Integer.parseInt(msgToSend[1]) / 2)); //targetNode
		String tempp=null;
		try {
			tempp= genHash(Integer.toString(Integer.parseInt(targetNode)/2));
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		Log.i("getFilesFromSuccessor"," target node is"+targetNode+ "  hashed vallue is: "+ tempp );
		int indexS=0;
		for(int i=0;i<nodeList.size();i++){
			if(tempp.equals(nodeList.get(i))){
				indexS=i;
				break;
			}
		}
		Log.i("getFilesFromSuccessor:"," index found is: "+indexS);
		if(indexS==(nodeList.size()-1)) {
			nextPort = dhtNodes.get(nodeList.get(0));
			nextPort2 = dhtNodes.get(nodeList.get(1));
		}
		else if(indexS==(nodeList.size()-2)) {
			nextPort = dhtNodes.get(nodeList.get(indexS + 1));
			nextPort2 = dhtNodes.get(nodeList.get(0));
		} else {
			nextPort = dhtNodes.get(nodeList.get(indexS + 1));
			nextPort2 = dhtNodes.get(nodeList.get(indexS + 2));
		}
		String[] ports={targetNode,nextPort,nextPort2};

		try{
			for(String port:ports) {
				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(port));

					PrintStream printOut = null;
					BufferedReader inReader = null;
					printOut = new PrintStream(socket.getOutputStream());
					inReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					printOut.println(str);
					printOut.flush();
					line = inReader.readLine();
					socket.close();
				} catch(SocketTimeoutException e){
					socket.close();
					e.printStackTrace();
					continue;
				} catch(NullPointerException e){
					socket.close();
					e.printStackTrace();
					continue;
				} catch (UnknownHostException e) {
					socket.close();
					e.printStackTrace();
					continue;
				} catch(IOException e){
					socket.close();
					e.printStackTrace();
					continue;
				} catch(Exception e){
					socket.close();
					e.printStackTrace();
					continue;
				}
			}

		}catch (Exception e){
			e.printStackTrace();
		}
	}
	private void delFilesFromSuccessor1(String str, String targetNode){
		Socket socket=null;
		String line="";
		try{
				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(targetNode));

					PrintStream printOut = null;
					BufferedReader inReader = null;
					printOut = new PrintStream(socket.getOutputStream());
					inReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					printOut.println(str);
					printOut.flush();
					line = inReader.readLine();
					socket.close();
				} catch(SocketTimeoutException e){
					socket.close();
					e.printStackTrace();
				} catch(NullPointerException e){
					socket.close();
					e.printStackTrace();
				} catch (UnknownHostException e) {
					socket.close();
					e.printStackTrace();
				} catch(IOException e){
					socket.close();
					e.printStackTrace();
				} catch(Exception e){
					socket.close();
					e.printStackTrace();
				}
		}catch (Exception e){
			e.printStackTrace();
		}
	}

	private void delFromPrefNodes(String selection){
		delFilesFromSuccessor1(("del&"+"-"+selection),this.dhtNodes.get(this.preferenceList.get(0)));
		delFilesFromSuccessor1(("del&"+"-"+selection),this.dhtNodes.get(this.preferenceList.get(1)));
	}

	private void delSelFromPref(String selection){
		File dir = getContext().getFilesDir();
		String trgtNode = targetNode(selection);
		try {
			for (File file : dir.listFiles()) {
				if (file.getName().equals(selection)) {
					this.keyToNode.remove(selection);
					List<String> temp = this.NodeToKey.get(trgtNode);
					for(int i=0;i<temp.size();i++){
						if(temp.get(i).equals(selection)){
							temp.remove(i);
							break;
						}
					}
					this.NodeToKey.put(trgtNode,temp);
					file.delete();
					break;
				}
			}
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
	}


	private void delFileFromAvd(String selection){
		String trgtNode = targetNode(selection);
		if(trgtNode.equals(this.myPortt)) {
			File dir = getContext().getFilesDir();
			try {
				for (File file : dir.listFiles()) {
					if (file.getName().equals(selection)) {
//						Log.i("del","file: "+file.getName()+"  deleted");
						delFromPrefNodes(selection);
						this.keyToNode.remove(selection);
						List<String> temp = this.NodeToKey.get(this.myPortt);
						for(int i=0;i<temp.size();i++){
							if(temp.get(i).equals(selection)){
								temp.remove(i);
								break;
							}
						}
						this.NodeToKey.put(this.myPortt,temp);
						file.delete();
						break;
					}
				}
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
		}
		else{
			Log.i("del","for key: "+selection+" taget node found is: "+trgtNode+"  this avd is: "+this.myPortt);
			delFilesFromSuccessor(("del@"+"-"+selection),trgtNode);
		}
	}

	private void delFileFromAvd1(String selection){
			File dir = getContext().getFilesDir();
			try {
				for (File file : dir.listFiles()) {
					if (file.getName().equals(selection)) {
//						Log.i("del","file: "+file.getName()+"  deleted");
						delFromPrefNodes(selection);
						this.keyToNode.remove(selection);
						List<String> temp = this.NodeToKey.get(this.myPortt);
						for(int i=0;i<temp.size();i++){
							if(temp.get(i).equals(selection)){
								temp.remove(i);
								break;
							}
						}
						this.NodeToKey.put(this.myPortt,temp);
						file.delete();
						break;
					}
				}
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
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub

		if (selection.equals("*")){
			delFilesfromAllServers();
		} else if(selection.equals("@")) {
			delAllFilesFromAvd();
		} else {
			Log.i("del","single file to be deleted: "+selection+" request came at: "+this.myPortt);
			delFileFromAvd(selection);
		}

		return 0;
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

    private void insertAtPrefNode(String msgtobestored){
		String[] message = msgtobestored.split("\\-");
		FileOutputStream outputStream =   null;
		try {
			outputStream = getContext().openFileOutput(message[0], Context.MODE_PRIVATE);
			outputStream.write((message[1].split("\\.")[0]).getBytes());
			outputStream.close();

			if(!(this.keyToNode.containsKey(message[0]))){ // this avd does not contain  this msg
				this.keyToNode.put(message[0],message[1].split("\\.")[1]);
				if(this.NodeToKey.containsKey(message[1].split("\\.")[1])){
					List<String> temp = this.NodeToKey.get(message[1].split("\\.")[1]);
					temp.add(message[0]);
					this.NodeToKey.put(message[1].split("\\.")[1],temp);
				} else {
					List<String> temp = new ArrayList<String>();
					temp.add(message[0]);
					this.NodeToKey.put(message[1].split("\\.")[1],temp);
				}
			}
			Log.i("Inserted at pref node", "  port: " + this.myPortt + "  inserted key : " + message[0] + "  value: " + message[1].split("\\.")[0]+"  coordinator is: "+message[1].split("\\.")[1]);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	//server thread
	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		private Uri buildObj1;
		private ContentValues contValObj;
		private ContentResolver contentResolverObj;

		private void UriFunction(String strReceived1){
			String[] strReceived = strReceived1.split("\\-");
			try{
				buildObj1 = makeUriObj();
				contValObj = new ContentValues();
				contValObj.put("value", strReceived[1]);
				contValObj.put("key", strReceived[0]);
				contentResolverObj =   getContext().getContentResolver();
				//Log.i("server","insedie uri function key is: "+strReceived[0]+"  msg is: "+strReceived[1]);
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
					String inputMsgs = inReader.readLine();
					String[] inMessage = inputMsgs.split("\\-");
					//String OpMsg = "insertPref"+"-"+ dhtNodes.get(preferenceList.get(0))+"-"+keyMsg+"-"+line11;
					if(inMessage[0].equals("insertWAVD") ) {
						Log.i("server", "Insert on avd: "+inMessage[1] + " key: "+ inMessage[2]+  " msg "+inMessage[3]);
						String[] inMessageBreak = inMessage[3].split("\\.");
						if (inMessageBreak[1].equals(myPortt)){
							UriFunction(inMessage[2]+"-"+inMessage[3]);
							printOut.println("done!!");
						} else {
							insertAtPrefNode(inMessage[2]+"-"+inMessage[3]);
							printOut.println("done!!");
						}
					}else if(inMessage[0].equals("insertPref")){
						Log.i("server", "Insert on avd: "+inMessage[1] + " key: "+ inMessage[2]+  " msg "+inMessage[3]);
						insertAtPrefNode(inMessage[2]+"-"+inMessage[3]);
//						UriFunction2(inMessage[2]+"-"+inMessage[3]);
						printOut.println("done!!");
					}else if (inMessage[0].equals("$")) {
						buildObj1 = makeUriObj();
						Log.e("Server query", "Requst came on avd: "+myPortt+" type of query: "+inMessage[1]);
						MatrixCursor mx = (MatrixCursor)  query(buildObj1,null, inMessage[1], null, null);
						String line1="";
						mx.moveToFirst();
						line1 += mx.getString(0)+"-"+mx.getString(1);
						printOut.println(line1);
					} else if(inMessage[0].equals("*")) {
						buildObj1 = makeUriObj();
						Log.e("Server query", "Requst came on avd: "+myPortt+" type of query: "+inMessage[0]);
						MatrixCursor mx = (MatrixCursor)  query(buildObj1,null, inMessage[1], null, null);
						String line1="";
						mx.moveToFirst();
						while(!mx.isAfterLast()){
							line1 += mx.getString(0)+"-"+mx.getString(1)+"|";
							mx.moveToNext();
							Log.e("at *","while loop");
						}
						printOut.println(line1);
					} else if (inMessage[0].equals("del@")){
						delFileFromAvd1(inMessage[1]);
						printOut.println("done!!");
					} else if (inMessage[0].equals("del*")){
						delAllFilesFromAvd();
						printOut.println("done!!");
					} else if (inMessage[0].equals("del&")){
						delSelFromPref(inMessage[1]);
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
				socket.setSoTimeout(1000);
				Log.i("client:","from avd: "+myPortt+" msg is being sent to : "+msgToSend[1]+" type of msg is: "+msgToSend[0]);
				PrintStream printOut = null;
				BufferedReader inReader = null;
				printOut = new PrintStream(socket.getOutputStream());
				inReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				printOut.println(msgs[0]);
				printOut.flush();
				inReader.readLine();
				socket.close();
			} catch(SocketTimeoutException e){
				e.printStackTrace();
			} catch(NullPointerException e){
				e.printStackTrace();
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch(IOException e){
				e.printStackTrace();
			} catch(Exception e){
				e.printStackTrace();
			}
			return null;
		}
	}

	private class ClientTask1 extends AsyncTask<String, Void, Void> {

		String nextPort="";
		String nextPort2="";
		Socket socket=null;

		@Override
		protected Void doInBackground(String... msgs) {
			try{

				String msgToSend[] = msgs[0].split("\\-");//0 1 2 3.l

				String temp = genHash(Integer.toString(Integer.parseInt(msgToSend[1]) / 2));//Integer.toString(Integer.parseInt(msgToSend[1]) / 2)
				Log.i("client1:"," target port:"+msgToSend[1]+" hashed value:"+temp);
				int indexS=0;
				for(int i=0;i<nodeList.size();i++){
					if(temp.equals(nodeList.get(i))){
						indexS=i;
						break;
					}
				}
				Log.i("client1:"," index found is: "+indexS);

				if(indexS==(nodeList.size()-1)) {
					nextPort = dhtNodes.get(nodeList.get(0));
					nextPort2 = dhtNodes.get(nodeList.get(1));
				}
				else if(indexS==(nodeList.size()-2)) {
					nextPort = dhtNodes.get(nodeList.get(indexS + 1));
					nextPort2 = dhtNodes.get(nodeList.get(0));
				} else {
					nextPort = dhtNodes.get(nodeList.get(indexS + 1));
					nextPort2 = dhtNodes.get(nodeList.get(indexS + 2));
				}
				String[] ports={msgToSend[1],nextPort,nextPort2};
				Log.i("client1", "key:"+msgToSend[2]+"  port1: "+msgToSend[1]+"   port2: "+nextPort+"   port3: "+nextPort2);
				for(String port:ports) {
					try {
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(port));
						socket.setSoTimeout(1000);
						Log.i("client1"," sending to: "+port);
//						Log.i("client1:", "from avd: " + myPortt + " msg is being sent to : " + msgToSend[1] + " type of msg is: " + msgToSend[0]);
						PrintStream printOut = null;
						BufferedReader inReader = null;
						printOut = new PrintStream(socket.getOutputStream());
						inReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
						printOut.println(msgs[0]);
						printOut.flush();
						inReader.readLine();
						inReader.close();
						printOut.close();
						socket.close();
//						break;
					} catch(SocketTimeoutException e){
						socket.close();
						e.printStackTrace();
						continue;
					} catch(NullPointerException e){
						socket.close();
						e.printStackTrace();
						continue;
					} catch (UnknownHostException e) {
						socket.close();
						e.printStackTrace();
						continue;
					} catch(IOException e){
						socket.close();
						e.printStackTrace();
						continue;
					} catch(Exception e){
						socket.close();
						e.printStackTrace();
						continue;
					}
				}
			} catch (Exception e){
				e.printStackTrace();
			}
			return null;
		}
	}

}
