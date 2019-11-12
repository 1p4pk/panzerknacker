package de.hpi.ddm.actors;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.apache.commons.lang3.Range;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.IOException;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.NotUsed;
import akka.util.ByteString;
import akka.stream.ActorMaterializer;
import akka.stream.IOResult;
import akka.stream.SourceRef;
import akka.stream.javadsl.StreamConverters;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.StreamRefs;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class LargeMessageProxy extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "largeMessageProxy";
	
	public static final int CHUNK_SIZE = 1024;
	
	public static Props props() {
		return Props.create(LargeMessageProxy.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class LargeMessage<T> implements Serializable {
		private static final long serialVersionUID = 2940665245810221108L;
		private T message;
		private ActorRef receiver;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class InitMessage implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private SourceRef byteStream;
		private ActorRef sender;
		private ActorRef receiver;
	}
	
	/////////////////
	// Actor State //
	/////////////////´
	
	private final ActorMaterializer mat = ActorMaterializer.create(this.getContext());
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	////////////////////
	// Actor Behavior //
	////////////////////
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(LargeMessage.class, this::handle)
				.match(InitMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(LargeMessage<?> message) {
		ActorRef receiver = message.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));
		
		try{
		Kryo kryo = new Kryo();
		Output out = new Output(new ByteArrayOutputStream());
        kryo.writeObject(out, message);
        
        ByteArrayInputStream bis = new ByteArrayInputStream(out.toBytes());
        Input in = new Input(bis);
        out.close();
        
        Source<ByteString, CompletionStage<IOResult>> source = StreamConverters.fromInputStream(() -> in, CHUNK_SIZE);
        in.close();
        @SuppressWarnings("unchecked")
		SourceRef<ByteString> sourceRef = (SourceRef<ByteString>) source.runWith(StreamRefs.sourceRef(), mat);
        
        receiverProxy.tell(new InitMessage(sourceRef, this.sender(), message.getReceiver()), this.self());
        
		} catch(IOException i) {
			i.printStackTrace();
		}	
	}
	
	private void handle(InitMessage message) {
		// Reassemble the message content, deserialize it and/or load the content from some local location before forwarding its content.
		message.getByteStream().getSource().runWith(
				Sink.foreach(bytes -> System.out.println((string) bytes)),
				mat);
	}
}
