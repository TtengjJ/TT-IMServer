package com.crossoverjie.cim.common.util;

import com.crossoverjie.cim.common.protocol.BaseCommand;
import com.crossoverjie.cim.common.protocol.Request;
import com.crossoverjie.cim.common.protocol.Response;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.Assert;
import org.junit.Test;

public class ProtocolTest {

    @Test
    public void testProtocol() throws InvalidProtocolBufferException {
        Request protocol = Request.newBuilder()
                .setRequestId(123L)
                .setReqMsg("你好啊")
                .setCmd(BaseCommand.LOGIN_REQUEST)
                .build();

        byte[] encode = encode(protocol);

        Request parseFrom = decode(encode);

        System.out.println(protocol);
        System.out.println(protocol.toString().equals(parseFrom.toString()));
    }

    /**
     * 编码
     * @param protocol protocol
     * @return byte array
     */
    public static byte[] encode(Request protocol) {
        return protocol.toByteArray();
    }

    /**
     * 解码
     * @param bytes bytes
     * @return Request
     * @throws InvalidProtocolBufferException exception
     */
    public static Request decode(byte[] bytes) throws InvalidProtocolBufferException {
        return Request.parseFrom(bytes);
    }

    @Test
    public void batchMessageIdAlignsWhenDecodedAsResponse() throws InvalidProtocolBufferException {
        Request request = Request.newBuilder()
                .setRequestId(100L)
                .setCmd(BaseCommand.OFFLINE)
                .addBatchReqMsg("a")
                .addBatchReqMsg("b")
                .addBatchMessageId(11L)
                .addBatchMessageId(12L)
                .build();

        Response response = Response.parseFrom(request.toByteArray());
        Assert.assertEquals(2, response.getBatchResMsgCount());
        Assert.assertEquals(2, response.getBatchMessageIdCount());
        Assert.assertEquals("a", response.getBatchResMsg(0));
        Assert.assertEquals(11L, response.getBatchMessageId(0));
        Assert.assertEquals("b", response.getBatchResMsg(1));
        Assert.assertEquals(12L, response.getBatchMessageId(1));
    }
}

