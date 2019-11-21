package de.hpi.ddm.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import de.hpi.ddm.MasterSystem;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.ArrayUtils;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Worker extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "worker";

	public static Props props() {
		return Props.create(Worker.class);
	}

	public Worker() {
		this.cluster = Cluster.get(this.context().system());
	}
	
	////////////////////
	// Actor Messages //
	////////////////////

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class HintSetupMessage implements Serializable {
		private static final long serialVersionUID = -50375816448627600L;
		private char resultChar;
		private char[] alphabet;
		private int amountHints;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class HintDataMessage implements Serializable {
		private static final long serialVersionUID = -50375816444623600L;
		private Map<String, String> hintData;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class PasswordDataMessage implements Serializable {
		private static final long serialVersionUID = -50375819032223600L;
		private String id;
		private String passwordHash;
		private char[] passwordAlphabet;
		private int passwordLength;
	}
	/////////////////
	// Actor State //
	/////////////////

	private Member masterSystem;
	private final Cluster cluster;

	// Hint variables
	private char resultChar;
	private char[] alphabet;
	private int amountHints;
	private Map<String, String> curentHints;

	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
		
		this.cluster.subscribe(this.self(), MemberUp.class, MemberRemoved.class);
	}

	@Override
	public void postStop() {
		this.cluster.unsubscribe(this.self());
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(CurrentClusterState.class, this::handle)
				.match(MemberUp.class, this::handle)
				.match(MemberRemoved.class, this::handle)
				.match(HintSetupMessage.class, this::handle)
				.match(HintDataMessage.class, this::handle)
				.match(PasswordDataMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(CurrentClusterState message) {
		message.getMembers().forEach(member -> {
			if (member.status().equals(MemberStatus.up()))
				this.register(member);
		});
	}

	private void handle(MemberUp message) {
		this.register(message.member());
	}

	private void register(Member member) {
		if ((this.masterSystem == null) && member.hasRole(MasterSystem.MASTER_ROLE)) {
			this.masterSystem = member;
			
			this.getContext()
				.actorSelection(member.address() + "/user/" + Master.DEFAULT_NAME)
				.tell(new Master.RegistrationMessage(), this.self());
		}
	}
	
	private void handle(MemberRemoved message) {
		if (this.masterSystem.equals(message.member()))
			this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
	}

	private void handle(HintSetupMessage message) {
		this.resultChar = message.getResultChar();
		this.alphabet = message.getAlphabet();
		this.amountHints = message.getAmountHints();
		this.sender().tell(new Master.PullDataMessage(), this.self());
	}

	private void handle(HintDataMessage message) {
		this.curentHints = message.getHintData();
		char[] hintAlphabet = ArrayUtils.removeElement(this.alphabet, this.resultChar);
		int hintLength = this.alphabet.length - 1;
		List<String> possibleCleartextHints = new ArrayList<>();
		this.heapPermutation(hintAlphabet, hintLength, possibleCleartextHints);

		for(String cleartextHint: possibleCleartextHints) {
			String hash = this.hash(cleartextHint);
			if (curentHints.containsKey(hash)){
				String passwordId = curentHints.get(hash);
				this.sender().tell(new Master.HintResultMessage(passwordId, resultChar), this.self());
				this.log().info("Cracked a hint for char " + resultChar);
			}
		}
		this.sender().tell(new Master.PullDataMessage(), this.self());
	}

	private void handle(PasswordDataMessage message) {
		this.log().info("Start cracking the password");
		List<String> possibleCleartextPasswords = new ArrayList<>();
		this.heapPermutation(message.getPasswordAlphabet(), message.getPasswordLength(), possibleCleartextPasswords);
        this.log().info("Finished permutation generation");

		for(String cleartextPassword: possibleCleartextPasswords) {
			String hash = this.hash(cleartextPassword);
			if (hash.equals(message.getPasswordHash())){
				this.sender().tell(new Master.PasswordResultMessage(message.getId(), cleartextPassword), this.self());
				this.log().info(String.format("Cracked password no. {}: {}", message.getId(), cleartextPassword));
			}
		}
	}

	private String hash(String line) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] hashedBytes = digest.digest(String.valueOf(line).getBytes(StandardCharsets.UTF_8));
			
			StringBuffer stringBuffer = new StringBuffer();
			for (int i = 0; i < hashedBytes.length; i++) {
				stringBuffer.append(Integer.toString((hashedBytes[i] & 0xff) + 0x100, 16).substring(1));
			}
			return stringBuffer.toString();
		}
		catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e.getMessage());
		}
	}
	
	// Generating all permutations of an array using Heap's Algorithm
	// https://en.wikipedia.org/wiki/Heap's_algorithm
	// https://www.geeksforgeeks.org/heaps-algorithm-for-generating-permutations/
	private void heapPermutation(char[] a, int size, List<String> l) {
		// If size is 1, store the obtained permutation
		if (size == 1)
			l.add(new String(a));

		for (int i = 0; i < size; i++) {
			heapPermutation(a, size - 1, l);

			// If size is odd, swap first and last element
			if (size % 2 == 1) {
				char temp = a[0];
				a[0] = a[size - 1];
				a[size - 1] = temp;
			}

			// If size is even, swap i-th and last element
			else {
				char temp = a[i];
				a[i] = a[size - 1];
				a[size - 1] = temp;
			}
		}
	}
}