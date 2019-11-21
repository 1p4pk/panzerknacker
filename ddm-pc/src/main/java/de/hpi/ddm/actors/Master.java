package de.hpi.ddm.actors;

import akka.actor.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.ArrayUtils;

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
        this.unassignedPasswords = new ArrayList<>();
        this.remainingAlphabetForId = new HashMap<>();
		this.hashedPasswords = new HashMap<>();
		this.resultPasswords = new HashMap<>();
		this.currentBatchId = 0;
		this.lastPasswordId = 0;
		this.charBatchMap = new HashMap<>();
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
    private final List<Worker.PasswordDataMessage> unassignedPasswords;


	private long startTime;

	private boolean isInit = false;
	private int passwordLength;
	private char[] alphabet;
	private int amountHints;

	private Map<String, String> hashedHints;
	private Map<String, String> hashedPasswords;
	private Map<String, String> resultPasswords;
	private int lastPasswordId;
	private int currentBatchId;
	private Map<Character, Integer> charBatchMap;

	private Map<String, char[]> remainingAlphabetForId;
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
                .match(PasswordResultMessage.class, this::handle)
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
		///////////////////////////////////////////////////////////////////////////////////////////////////////
		
		if (message.getLines().isEmpty()) {
			this.collector.tell(new Collector.PrintMessage(), this.self());
//			this.terminate();
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
				this.charBatchMap.put(varChar, 0);

			}

			for(ActorRef worker : this.workers){
				if(!this.unassignedHintChars.isEmpty()){
                    Worker.HintSetupMessage workSetup = this.unassignedHintChars.remove(0);
					worker.tell(workSetup, this.self());
					this.charWorkers.put(worker, workSetup);
				}
			}
		}

		this.currentBatchId++;

		this.hashedHints = new HashMap<>();
		for(String[] line : message.getLines()){
			this.hashedPasswords.put(line[0], line[4]);
			for(int i = 5; i < this.amountHints + 5; i++){
				this.hashedHints.put(line[i], line[0]);
			}
		}

		for(ActorRef charWorker : this.idleHintCrackers){
			charWorker.tell(new Worker.HintDataMessage(this.hashedHints), this.self());
			this.charBatchMap.put(this.charWorkers.get(charWorker).getResultChar(), this.currentBatchId);
		}
		
		this.collector.tell(new Collector.CollectMessage("Processed batch of size " + message.getLines().size()), this.self());
	}

	protected void handle(PullDataMessage message) {
		char currentChar = this.charWorkers.get(this.sender()).getResultChar();
		if (this.charBatchMap.get(currentChar) < this.currentBatchId) {
			Map<String, String> hintMessageData = this.hashedHints;
			this.charBatchMap.put(currentChar, this.currentBatchId);
			this.sender().tell(new Worker.HintDataMessage(hintMessageData), this.self());
		} else if (!this.unassignedPasswords.isEmpty()) {
			// TODO: remove first element not last (crack passwords in order)
			Worker.PasswordDataMessage unassignedPassword = this.unassignedPasswords.remove(0);
			this.unassignedHintChars.add(new Worker.HintSetupMessage(currentChar, this.alphabet, this.amountHints));
			this.charWorkers.remove(this.sender());
			this.sender().tell(unassignedPassword, this.self());
		} else if (!this.unassignedHintChars.isEmpty()) {
			Worker.HintSetupMessage workSetup = this.unassignedHintChars.remove(0);
			this.sender().tell(workSetup, this.self());
			this.charWorkers.put(this.sender(), workSetup);
			this.unassignedHintChars.add(new Worker.HintSetupMessage(currentChar, this.alphabet, this.amountHints));
		} else {
			this.idleHintCrackers.add(this.sender());
		}
		boolean batchFinished = true;
		for (int id : this.charBatchMap.values()) {
			if (id < this.currentBatchId) {
				batchFinished = false;
				break;
			}
		}
		if (batchFinished) { this.reader.tell(new Reader.ReadMessage(), this.self()); }
	}

	protected void handle(HintResultMessage message){
		String id = message.getId();
		char hint = message.getNonContainedChar();
		if(this.remainingAlphabetForId.containsKey(id)){
			this.remainingAlphabetForId.put(id, ArrayUtils.removeElement(this.remainingAlphabetForId.get(id), hint));
            if (this.alphabet.length - this.amountHints == this.remainingAlphabetForId.get(id).length) {
                char[] passwordAlphabet = this.remainingAlphabetForId.remove(id);

                Worker.PasswordDataMessage unassignedPassword = new Worker.PasswordDataMessage(
                        id, this.hashedPasswords.remove(id),
                        passwordAlphabet, this.passwordLength);

                if (!this.workers.isEmpty()) {
                    ActorRef passwordWorker = this.workers.remove(0);
                    passwordWorker.tell(unassignedPassword, this.self());
                } else {
                    // not enough workers at the moment
                    this.unassignedPasswords.add(unassignedPassword);
                }

            }
		} else {
			this.remainingAlphabetForId.put(id, ArrayUtils.removeElement(this.alphabet, hint));
		}
	}

    protected void handle(PasswordResultMessage message) {
		this.workers.add(this.sender());
		if(Integer.toString(this.lastPasswordId + 1) == message.getId()){
            this.collector.tell(new Collector.CollectMessage(message.getPw()), this.self());
            this.lastPasswordId++;
            if(!this.resultPasswords.isEmpty()){
                this.checkToSendPasswords();
            }
        } else {
            this.resultPasswords.put(message.getId(), message.getPw());
        }
    }

    private void checkToSendPasswords() {
	    String id = Integer.toString(this.lastPasswordId + 1);
        if(this.resultPasswords.containsKey(id)){
            this.collector.tell(new Collector.CollectMessage(this.resultPasswords.remove(id)), this.self());
            this.lastPasswordId++;
            if(!this.resultPasswords.isEmpty()){
                this.checkToSendPasswords();
            }
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
		if(!this.unassignedHintChars.isEmpty()){
            Worker.HintSetupMessage workSetup = this.unassignedHintChars.remove(0);
			this.sender().tell(workSetup, this.self());
			this.charWorkers.put(this.sender(), workSetup);
			this.charBatchMap.put(workSetup.getResultChar(), 0);
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
}
