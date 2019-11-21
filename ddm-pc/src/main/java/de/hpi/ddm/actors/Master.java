package de.hpi.ddm.actors;

import akka.actor.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.*;

public class Master extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "master";

	public static Props props(final ActorRef reader, final ActorRef collector) {
		return Props.create(Master.class, () -> new Master(reader, collector));
	}

	public Master(final ActorRef reader, final ActorRef collector) {
		this.reader = reader;
		this.collector = collector;
		this.workers = new ArrayList<>();
		this.charWorkers = new HashMap<>();
		this.unassignedHintChars = new ArrayList<>();
		this.idleHintCrackers = new ArrayList<>();
		this.hintResults = new HashMap<>();
		this.currentBatchId = 0;
		this.workerBatchMap = new HashMap<>();
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data
	public static class StartMessage implements Serializable {
		private static final long serialVersionUID = -50374816448627600L;
	}
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BatchMessage implements Serializable {
		private static final long serialVersionUID = 8343040942748609598L;
		private List<String[]> lines;
	}

	@Data
	public static class RegistrationMessage implements Serializable {
		private static final long serialVersionUID = 3303081601659723997L;
	}

	@Data
	public static class PullDataMessage implements Serializable {
		private static final long serialVersionUID = 3303382301659723997L;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class HintResultMessage implements Serializable {
		private static final long serialVersionUID = 3303382394659723997L;
		private String id;
		private char nonContainedChar;
	}

    @Data @NoArgsConstructor @AllArgsConstructor
    public static class PasswordResultMessage implements Serializable {
        private static final long serialVersionUID = 3303382392345723997L;
        private String id;
        private String pw;
    }
	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef reader;
	private final ActorRef collector;
	private final List<ActorRef> workers;
    private final Map<ActorRef, Worker.HintSetupMessage> charWorkers;
    private final List<Worker.HintSetupMessage> unassignedHintChars;
	private final List<ActorRef> idleHintCrackers;


	private long startTime;

	private boolean isInit = false;
	private int passwordLength;
	private char[] alphabet;
	private int amountHints;

	private List<String[]> currentBatch;
	private int currentBatchId;
	private Map<ActorRef, Integer> workerBatchMap;

	private Map<String, char[]> hintResults;
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(StartMessage.class, this::handle)
				.match(BatchMessage.class, this::handle)
				.match(PullDataMessage.class, this::handle)
				.match(HintResultMessage.class, this::handle)
				.match(Terminated.class, this::handle)
				.match(RegistrationMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	protected void handle(StartMessage message) {
		this.startTime = System.currentTimeMillis();
		
		this.reader.tell(new Reader.ReadMessage(), this.self());
	}
	
	protected void handle(BatchMessage message) {
		
		///////////////////////////////////////////////////////////////////////////////////////////////////////
		// The input file is read in batches for two reasons: /////////////////////////////////////////////////
		// 1. If we distribute the batches early, we might not need to hold the entire input data in memory. //
		// 2. If we process the batches early, we can achieve latency hiding. /////////////////////////////////
		// TODO: Implement the processing of the data for the concrete assignment. ////////////////////////////
		///////////////////////////////////////////////////////////////////////////////////////////////////////
		
		if (message.getLines().isEmpty()) {
			this.collector.tell(new Collector.PrintMessage(), this.self());
			this.terminate();
			return;
		}

		if(!this.isInit){
			this.isInit = true;
			String[] firstRow = message.getLines().get(0);
			this.passwordLength = Integer.parseInt(firstRow[3]);
			this.alphabet = firstRow[2].toCharArray();
			this.amountHints = firstRow.length - 5;
			for(char varChar : alphabet){
                unassignedHintChars.add(new Worker.HintSetupMessage(varChar, this.alphabet, this.amountHints));
			}

			for(ActorRef worker : this.workers){
				if(!this.unassignedHintChars.isEmpty()){
                    Worker.HintSetupMessage workSetup = this.unassignedHintChars.remove(this.unassignedHintChars.size() - 1);
					worker.tell(workSetup, this.self());
					this.charWorkers.put(worker, workSetup);
					this.workerBatchMap.put(worker, 0);
				}
			}
		}

		this.currentBatchId++;
		this.currentBatch = message.getLines();

		Map<String, String> hintMessageData = this.convertBatchToHintMap();

		for(ActorRef charWorker : this.idleHintCrackers){
			charWorker.tell(new Worker.HintDataMessage(hintMessageData), this.self());
			this.workerBatchMap.put(charWorker, this.currentBatchId);
		}
		
		this.collector.tell(new Collector.CollectMessage("Processed batch of size " + message.getLines().size()), this.self());
	}

	protected void handle(PullDataMessage message) {
		if(this.workerBatchMap.get(this.sender()) < this.currentBatchId){
			Map<String, String> hintMessageData = this.convertBatchToHintMap();
			this.workerBatchMap.put(this.sender(), this.currentBatchId);
			this.sender().tell(new Worker.HintDataMessage(hintMessageData), this.self());
		} else {
			this.idleHintCrackers.add(this.sender());
		}

		if (this.unassignedHintChars.isEmpty()){
			boolean readNewBatch = true;
			for(Integer  batchId : this.workerBatchMap.values()) {
				if (batchId < this.currentBatchId){
					readNewBatch=false;
					break;
				}
			}
			if (readNewBatch) {this.reader.tell(new Reader.ReadMessage(), this.self());}
		}

	}

	protected void handle(HintResultMessage message){

		if(this.hintResults.containsKey(message.getId())){
			char[] charArray = this.hintResults.get(message.getId());
			char[] updateArray = Arrays.copyOf(charArray, charArray.length+1);
			updateArray[charArray.length] = message.getNonContainedChar();
			this.hintResults.put(message.getId(), updateArray);
            // TODO: Start cracking pw
            if (this.amountHints == updateArray.length) {
                // TODO: Notify pw-cracker worker
                // TODO: Delete hints from hint results
            }
		} else {
			this.hintResults.put(message.getId(), new char[]{message.getNonContainedChar()});
		}
	}

    protected void handle(PasswordResultMessage message) {
        // TODO: Receive cracked pw
    }

	protected void terminate() {
		this.reader.tell(PoisonPill.getInstance(), ActorRef.noSender());
		this.collector.tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		for (ActorRef worker : this.workers) {
			this.context().unwatch(worker);
			worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
		}
		
		this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		long executionTime = System.currentTimeMillis() - this.startTime;
		this.log().info("Algorithm finished in {} ms", executionTime);
	}

	protected void handle(RegistrationMessage message) {
		this.context().watch(this.sender());
		if(!this.unassignedHintChars.isEmpty()){
            Worker.HintSetupMessage workSetup = this.unassignedHintChars.remove(this.unassignedHintChars.size() - 1);
			this.sender().tell(workSetup, this.self());
			this.charWorkers.put(this.sender(), workSetup);
			this.workerBatchMap.put(this.sender(), 0);
		} else {
			this.workers.add(this.sender());
		}
		this.log().info("Registered {}", this.sender());
	}
	
	protected void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		this.workers.remove(message.getActor());
		this.log().info("Unregistered {}", message.getActor());
	}

	protected Map<String, String> convertBatchToHintMap(){
		Map<String, String> hintMessageData = new HashMap<>();
		for(String[] line : this.currentBatch){
			for(int i = 5; i < this.amountHints + 5; i++){
				hintMessageData.put(line[i], line[0]);
			}
		}
		return hintMessageData;
	}
}
