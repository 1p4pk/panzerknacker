package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.*;

import akka.actor.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

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
		this.unassignedWork = new ArrayList<>();
		this.pullMessages = 0;
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

	@Data
	public static class HintResultMessage implements Serializable {
		private static final long serialVersionUID = 3303382394659723997L;
		private String id;
		private char nonContainedChar;
	}
	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef reader;
	private final ActorRef collector;
	private final List<ActorRef> workers;
	private final Map<ActorRef, Worker.SetupMessage> charWorkers;
	private final List<Worker.SetupMessage> unassignedWork;


	private long startTime;

	private boolean isInit = false;
	private int passwordLength;
	private char[] alphabet;
	private int amountHints;
	private List<String[]> currentBatch;
	private int pullMessages;
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
				unassignedWork.add(new Worker.SetupMessage(varChar, this.alphabet, this.amountHints));
			}

			for(ActorRef worker : this.workers){
				if(this.unassignedWork.size() > 0){
					Worker.SetupMessage workSetup = this.unassignedWork.remove(this.unassignedWork.size() - 1);
					worker.tell(workSetup, this.self());
					this.charWorkers.put(worker, workSetup);
				}
			}
		}

		this.currentBatch = message.getLines();

		for(ActorRef charWorker : this.charWorkers.keySet()){
			charWorker.tell(new Worker.HintDataMessage(this.currentBatch));
		}
		
		this.collector.tell(new Collector.CollectMessage("Processed batch of size " + message.getLines().size()), this.self());

	}

	protected void handle(PullDataMessage message) {
		this.pullMessages++;
		if(this.pullMessages == this.charWorkers.size()){
			this.reader.tell(new Reader.ReadMessage(), this.self());
			this.pullMessages = 0;
		}
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
		if(this.unassignedWork.size() > 0){
			Worker.SetupMessage workSetup = this.unassignedWork.remove(this.unassignedWork.size() - 1);
			this.sender().tell(workSetup, this.self());
			this.charWorkers.put(this.sender(), workSetup);
		} else {
			this.workers.add(this.sender());
		}
		//		this.log().info("Registered {}", this.sender());
	}
	
	protected void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		this.workers.remove(message.getActor());
//		this.log().info("Unregistered {}", message.getActor());
	}
}
