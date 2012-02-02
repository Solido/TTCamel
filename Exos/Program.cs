/*
 * Created by SharpDevelop.
 * User: admin
 * Date: 11/12/2011
 * Time: 11:10
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using System;
using Apache.NMS;
using Apache.NMS.Util;
using Apache.NMS.ActiveMQ;
using Apache.NMS.ActiveMQ.Commands;
using System.IO;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
//using System.Timers;
using System.Text;
using System.Net;

namespace Exos
{
	class ExosMQ
	{
		
		private static ExosMQ exos;
		private static Hashtable depths;
		
		private IConnection connection;
		private ISession session;
		private IDestination qtt_fills;
		private IDestination qtt_quotes;
		
		private XTAPI.TTInstrNotifyClass m_TTInstrNotify;

		private string baseFolder = AppDomain.CurrentDomain.BaseDirectory;
		private string realTimeLocation;

		private XTAPI.TTOrderSetClass m_TTOrderSet = null;
		
		private long quotes_counter = 0;
		private static int QUOTE_BUFFER = 2000;
		private ArrayList quotesBuffer = new ArrayList(QUOTE_BUFFER);
		
		
		public static void Main(string[] args)
		{
			Thread.CurrentThread.Priority = ThreadPriority.Highest;
			exos = new ExosMQ();
			exos.run();
		}
		
		
		public void run() {
			
			log("started");
			
			IConnectionFactory factory = new ConnectionFactory();
			connection = factory.CreateConnection();
			connection.Start();
			session = connection.CreateSession();
			
			qtt_fills = session.GetQueue("TT_Fills");
			qtt_quotes = session.GetQueue("TT_Quotes");
			
			//System.Timers.Timer exportDepth = new System.Timers.Timer();
			//exportDepth.Interval = 10000;
			//exportDepth.Elapsed += new System.Timers.ElapsedEventHandler(ExportDepths);
			//exportDepth.Enabled = true;
			//exportDepth.Start();
			
			configure();
			
			while(true) {
				Thread.Sleep(200);
			}
			
		}
		
		
		private void configure() {
			
			realTimeLocation = Path.Combine(baseFolder,"RealTime.info");
			
			initQuotes();
			initFills();
			
			log("Configuring completed");
			
			
		}
		

		void ShutDown()
		{
			session.Close();
			connection.Close();
		}
		
		
		private void initFills(){
			
			log("Init Fills config");
			
			// Instantiate the TTOrderSet object.
			m_TTOrderSet = new XTAPI.TTOrderSetClass();

			// Enable the fill updates.
			m_TTOrderSet.EnableOrderFillData = 1;

			// Subscribe to the fill events.
			m_TTOrderSet.OnOrderFillData += new XTAPI._ITTOrderSetEvents_OnOrderFillDataEventHandler(m_TTOrderSet_OnOrderFillData);
			m_TTOrderSet.OnOrderFillBlockEnd += new XTAPI._ITTOrderSetEvents_OnOrderFillBlockEndEventHandler(m_TTOrderSet_OnOrderFillBlockEnd);
			m_TTOrderSet.OnOrderFillBlockStart += new XTAPI._ITTOrderSetEvents_OnOrderFillBlockStartEventHandler(m_TTOrderSet_OnOrderFillBlockStart);
			
			m_TTOrderSet.Open(0);
			
		}
		
		// FillID should be the last field for later parsing
		string fillRequest="Contract,Exchange,Price,Qty,BuySell,FillType,OrderNo,SiteOrderKey,Date,TimeExec,TimeLocalUpd,IsIceberg,OrderTag,Series,ComboCode,FillKey";
		
		private void m_TTOrderSet_OnOrderFillData(XTAPI.TTFillObj pFillObj)
		{
			// Retrieve the fill information using the TTFillObj Get Properties.
			Array fillData = (Array) pFillObj.get_Get(fillRequest);
			Console.Write('*');
			FillMessage(fillData);

		}
		
		void FillMessage(Array fillData)
		{
			
			// Print the fill information to the screen.
			StringBuilder sb = new StringBuilder();
			sb.Append( (string)fillData.GetValue(0) + ",");
			sb.Append( (string)fillData.GetValue(1) + ",");
			sb.Append( (string)fillData.GetValue(2) + ",");
			sb.Append( Convert.ToString(fillData.GetValue(3)) + ",");
			sb.Append( (string)fillData.GetValue(4) + ",");
			sb.Append( (string)fillData.GetValue(5) + ",");
			sb.Append( (string)fillData.GetValue(6) + ",");
			sb.Append( (string)fillData.GetValue(7) + ",");
			sb.Append( Convert.ToString(fillData.GetValue(8)) + ",");
			sb.Append( Convert.ToString(fillData.GetValue(9)) + ",");
			sb.Append( Convert.ToString(fillData.GetValue(10)) + ",");
			sb.Append( Convert.ToString(fillData.GetValue(11)) + ",");
			sb.Append( (string)fillData.GetValue(12) + ",");
			sb.Append( (string)fillData.GetValue(13) + ",");
			sb.Append( (string)fillData.GetValue(14) + ",");
			sb.Append( (string)fillData.GetValue(15) + ",");

			using(IMessageProducer producer = session.CreateProducer(qtt_fills))
			{
				producer.DeliveryMode = MsgDeliveryMode.Persistent;
				ITextMessage request = session.CreateTextMessage(sb.ToString());
				request.NMSCorrelationID = "TT";
				request.Properties["id"] = (string)fillData.GetValue(15);
				producer.Send(request);

			}
			
		}
		
		
		private void m_TTOrderSet_OnOrderFillBlockStart(){
			//log("FillBlockStart");
		}

		
		private void m_TTOrderSet_OnOrderFillBlockEnd() {
			//log("FillBlockEnd");
		}

		private void initQuotes() {
			
			// Instantiate the instrument notification class.
			m_TTInstrNotify = new XTAPI.TTInstrNotifyClass();

			log("Loading Quotes config: " + realTimeLocation);
			
			string[] lines = System.IO.File.ReadAllLines(realTimeLocation);
			
			// Setup the instrument notification call back functions.
			m_TTInstrNotify.OnNotifyFound += new XTAPI._ITTInstrNotifyEvents_OnNotifyFoundEventHandler(this.m_TTInstrNotify_OnNotifyFound);
			m_TTInstrNotify.OnNotifyUpdate += new XTAPI._ITTInstrNotifyEvents_OnNotifyUpdateEventHandler(this.m_TTInstrNotify_OnNotifyUpdate);
			
			log("Init Depth config");
			m_TTInstrNotify.OnNotifyDepthData += new XTAPI._ITTInstrNotifyEvents_OnNotifyDepthDataEventHandler(pNotify_OnNotifyDepthData);
			m_TTInstrNotify.EnableDepthUpdates = 1;
			depths = new Hashtable(600);
			
			string[] conf = null;
			string raw = null;
			
			foreach(string line in lines) {
				try {
					
					raw = line;
					conf = line.Split(',');
					XTAPI.TTInstrObj m_TTInstrObj = new XTAPI.TTInstrObj();
					m_TTInstrObj.Exchange =conf[0];
					m_TTInstrObj.Product =conf[1];
					m_TTInstrObj.ProdType =conf[2];
					m_TTInstrObj.Contract =conf[3];
					
					m_TTInstrNotify.AttachInstrument(m_TTInstrObj);
					// 1 authorize depth
					//m_TTInstrObj.Open(1);
					m_TTInstrObj.Open(0);
					
					Console.WriteLine(conf[1]+" "+conf[3]+" Added");
					
				} catch (Exception e)
				{
					log("Error!!! >>>"+e.Message+" "+line);
				}
				
			}
			
			log(lines.Length+" Quotes Parsed");
			
		}
		
		private void pNotify_OnNotifyDepthData(XTAPI.TTInstrNotify pNotify, XTAPI.TTInstrObj pInstr)
		{
			

			Array data = (Array) pInstr.get_Get("Exchange,Product,Contract");
			
			string ex = (string)data.GetValue(0);
			string product = (string)data.GetValue(1);
			string contract = (string)data.GetValue(2);
			
			StringBuilder idBld = new StringBuilder();
			idBld.Append(ex+",");
			idBld.Append(product+",");
			idBld.Append(contract);

			String id = idBld.ToString();
			
			// Obtain the bid depth (Level based on user selection).
			Array dataArrayBid = (Array)pInstr.get_Get("BidDepth(0)#");

			// Test if depth exists.
			if (dataArrayBid != null)
			{
				// Iterate through the depth array.
				for (int i = 0; i <= dataArrayBid.GetUpperBound(0); i++)
				{
					// Break out of FOR LOOP if index value is null.
					if (dataArrayBid.GetValue(i,0) == null)
						break;
					
					String bidDepth = id+",B,"+dataArrayBid.GetValue(i,0);
					//Console.WriteLine(bidDepth+":::"+dataArrayBid.GetValue(i,1));
					depths.Add(bidDepth,dataArrayBid.GetValue(i,1));
					

				}
			}

			// Obtain the ask depth (Level based on user selection).
			Array dataArrayAsk = (Array) pInstr.get_Get("AskDepth(0)#");
			
			// Test if depth exists.
			if (dataArrayAsk != null)
			{
				// Iterate through the depth array.
				for (int i = 0; i <= dataArrayAsk.GetUpperBound(0); i++)
				{
					// Break out of FOR LOOP if index value is null.
					if (dataArrayAsk.GetValue(i,0) == null)
						break;
					
					String askDepth = id+",A,"+dataArrayAsk.GetValue(i,0);
					//Console.WriteLine(askDepth+":::"+dataArrayAsk.GetValue(i,1));
					depths.Add(askDepth,dataArrayAsk.GetValue(i,1));
					
					
				}
			}
			
		}
		
		
		private void log(String message) {
			string fill = DateTime.Now.ToString("")+": "+message;
			Console.WriteLine(fill);
		}
		
		private void m_TTInstrNotify_OnNotifyFound(XTAPI.TTInstrNotify pNotify, XTAPI.TTInstrObj pInstr)
		{
			register(pInstr);
		}
		
		private void m_TTInstrNotify_OnNotifyUpdate(XTAPI.TTInstrNotify pNotify, XTAPI.TTInstrObj pInstr)
		{
			register(pInstr);
		}
		
		
		private void register(XTAPI.TTInstrObj pInstr) {
			
			quotes_counter +=1;
			
			try {
				
				Array data = (Array) pInstr.get_Get("Exchange,Product,Contract,Open$,High$,Low$,Last$,Close$,Settle$");
				
				if(quotes_counter%250==0) {
					Console.Write('.');
				}
				
				quote(data);
				
			} catch (Exception e) {
				log("Register Error:"+">>"+e.Message);
			}
			
		}
		
		void quote(Array data)
		{
			
			string ex = (string)data.GetValue(0);
			string product = (string)data.GetValue(1);
			string contract = (string)data.GetValue(2);
			string open = (string)data.GetValue(3);
			string hi = (string)data.GetValue(4);
			string low = (string)data.GetValue(5);
			string last = (string)data.GetValue(6);
			string close = (string)data.GetValue(7);
			string settle = (string)data.GetValue(8);
			
			string key = ex+"_"+product+"_"+contract;
			
			StringBuilder sb = new StringBuilder();
			sb.Append(ex+",");
			sb.Append(product+",");
			sb.Append(contract+",");
			sb.Append(open+",");
			sb.Append(hi+",");
			sb.Append(low+",");
			sb.Append(last+",");
			sb.Append(close+",");
			sb.Append(settle);
			
			quotesBuffer.Add(sb.ToString());
			if(quotesBuffer.Count == QUOTE_BUFFER) {
				quotesMessage();
				quotesBuffer.Clear();
			}
			
			
		}
		
		private void quotesMessage() {
			
			log("sending messages quotes");
			
			using(IMessageProducer producer = session.CreateProducer(qtt_quotes))
			{
				
				StringBuilder sb = new StringBuilder();
				
				foreach(string quote in quotesBuffer) {
					sb.Append(quote);
					sb.Append("\n");
				}
				
				producer.DeliveryMode = MsgDeliveryMode.Persistent;
				ITextMessage request = session.CreateTextMessage(sb.ToString());
				request.NMSCorrelationID = "TT";
				producer.Send(request);

			}
			
		}
		
		private void ExportDepths(object source, System.Timers.ElapsedEventArgs e) {
			
			log(depths.Values.Count+" Exported depths");
			StringBuilder sb = new StringBuilder();
			foreach (DictionaryEntry entry in depths) {
				sb.Append(entry.Key+","+entry.Value+"\n");
				
			}
			
		}
		
		
	}
}