package com.spirit.netty;

import org.apache.log4j.Logger;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;


public class TcpClient {
	private static final Logger logger = Logger.getLogger(TcpClient.class);
	public static String HOST = "192.168.1.215";
	public static int PORT = 9999;
	
	public static Bootstrap bootstrap = getBootstrap();
	public static Channel channel = getChannel(HOST,PORT);
	/**
	 * 初始化Bootstrap
	 * @return
	 */
	public static final Bootstrap getBootstrap(){
		EventLoopGroup group = new NioEventLoopGroup();
		Bootstrap b = new Bootstrap();
		b.group(group).channel(NioSocketChannel.class);
		b.handler(new ChannelInitializer<Channel>() {
			@Override
			protected void initChannel(Channel ch) throws Exception {
				ChannelPipeline pipeline = ch.pipeline();
				pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 2, 0, 0));
				pipeline.addLast("frameEncoder", new LengthFieldPrepender(2));
				pipeline.addLast("decoder", new StringDecoder(CharsetUtil.UTF_8));
				pipeline.addLast("encoder", new StringEncoder(CharsetUtil.UTF_8));
				pipeline.addLast("handler", new TcpClientHandler());
			}
		});
		b.option(ChannelOption.SO_KEEPALIVE, true);
		return b;
	}

	public static final Channel getChannel(String host,int port){
		Channel channel = null;
		try {
			channel = bootstrap.connect(host, port).sync().channel();
		} catch (Exception e) {
			logger.error(String.format("连接Server(IP[%s],PORT[%s])失败", host,port),e);
			return null;
		}
		return channel;
	}

	public static void sendMsg(String msg) throws Exception {
		if(channel!=null){
			channel.writeAndFlush(msg).sync();
		}else{
			logger.warn("消息发送失败,连接尚未建立!");
		}
	}

    public static void main(String[] args) throws Exception {
		try {
			long t0 = System.nanoTime();
			for (int i = 0; i < 100000; i++) {
				TcpClient.sendMsg(i+"你好1");
			}
			long t1 = System.nanoTime();
			System.out.println((t1-t0)/1000000.0);
			
			
//			byte[] a= {0,49,19,1,13,2,1,97,98,118,97,98,99,0,1,0,1,0,1,0,0,0,18,0,0,0,32,0,0,1,98,67,98,-25,-113,66,-10,62,-6,67,106,59,-25,67,-84,-84,41,4,106};
//		
//			channel.writeAndFlush(a.toString()).sync();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}


