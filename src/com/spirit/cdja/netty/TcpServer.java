package com.spirit.cdja.netty;

import org.apache.log4j.Logger;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;

public class TcpServer {
	private static final Logger logger = Logger.getLogger(TcpServer.class);
	private static final String IP = "127.0.0.1";
	private static final int PORT = 9999;
	/**用于分配处理业务线程的线程组个数 */
	protected static final int BIZGROUPSIZE = Runtime.getRuntime().availableProcessors()*2;	//默认
	/** 业务出现线程大小*/
	//默认是CPU核数的二倍,需要根据情况手动配置
	protected static final int BIZTHREADSIZE = 4;
	private static final EventLoopGroup bossGroup = new NioEventLoopGroup(BIZGROUPSIZE);
	private static final EventLoopGroup workerGroup = new NioEventLoopGroup(BIZTHREADSIZE);
	protected static void run() throws Exception {
		ServerBootstrap b = new ServerBootstrap();
		b.group(bossGroup, workerGroup);
		//使用对象池
		b.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
		b.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
		b.channel(NioServerSocketChannel.class);
		
		b.childHandler(new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(SocketChannel ch) throws Exception {
				ChannelPipeline pipeline = ch.pipeline();
				pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 2, 0, 0));
				pipeline.addLast("frameEncoder", new LengthFieldPrepender(2));
				pipeline.addLast("decoder", new StringDecoder(CharsetUtil.UTF_8));
				pipeline.addLast("encoder", new StringEncoder(CharsetUtil.UTF_8));
				pipeline.addLast(new TcpServerHandler());
			}
		});
		

		b.bind(IP, PORT).sync();
		logger.info("TCP服务器已启动");
	}

	protected static void shutdown() {
		workerGroup.shutdownGracefully();
		bossGroup.shutdownGracefully();
	}

	public static void main(String[] args) throws Exception {
		logger.info("开始启动TCP服务器...");
		TcpServer.run();
//		TcpServer.shutdown();
	}
}