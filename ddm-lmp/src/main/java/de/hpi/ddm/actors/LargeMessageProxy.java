package de.hpi.ddm.actors;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;

import java.util.concurrent.CompletionStage;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;


import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.util.ByteString;
import akka.stream.IOResult;
import akka.stream.Materializer;
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
	public static class LargeMessage<T> extends Output implements Serializable {
		private static final long serialVersionUID = 2940665245810221108L;
		private T message;
		private ActorRef receiver;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class InitMessage implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private SourceRef<ByteString> byteStream;
		private ActorRef sender;
		private ActorRef receiver;
	}
	
	/////////////////
	// Actor State //
	/////////////////´
	
	private final Materializer mat = Materializer.createMaterializer(this.getContext());
	
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
		
		try {
		Kryo kryo = new Kryo();
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		Output out = new Output(bos);
        kryo.writeClassAndObject(out, message.getMessage());
        out.close();
        bos.close();
        
        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        Input in = new Input(bis);
        in.close();
        bis.close();

        Source<ByteString, CompletionStage<IOResult>> source = StreamConverters.fromInputStream(() -> in, CHUNK_SIZE);
		SourceRef<ByteString> sourceRef = source.runWith(StreamRefs.sourceRef(), mat);
        
        receiverProxy.tell(new InitMessage(sourceRef, this.sender(), message.getReceiver()), this.self());
		} catch(Exception e) {
			this.log().error(e.getMessage());
		}
	}
	
	private void handle(InitMessage message) {

		final CompletionStage<ByteString> byteStr = message.getByteStream().getSource().runWith(
				Sink.fold(ByteString.empty(), (byteString, next) -> byteString.concat(next)), mat);
		byteStr.whenCompleteAsync((str, o) -> handleCompleteMessage(str, message.getReceiver(), message.getSender()));
	}

	private void handleCompleteMessage(ByteString byteString, ActorRef receiver, ActorRef sender) {
		try {
		Kryo kryo = new Kryo();
		ByteArrayInputStream bis = new ByteArrayInputStream(byteString.toArray());
		Input in = new Input(bis);
		in.close();
		bis.close();

		receiver.tell(kryo.readClassAndObject(in), sender);
		} catch (IOException e) {
			this.log().error(e.getMessage());
		}
	}
}
