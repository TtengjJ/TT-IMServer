package com.crossoverjie.cim.route.service.impl;

import com.crossoverjie.cim.common.constant.Constants;
import com.crossoverjie.cim.common.enums.StatusEnum;
import com.crossoverjie.cim.common.res.BaseResponse;
import com.crossoverjie.cim.persistence.api.pojo.OfflineMsg;
import com.crossoverjie.cim.persistence.api.service.OfflineMsgStore;
import com.crossoverjie.cim.route.api.vo.res.CIMServerResVO;
import com.crossoverjie.cim.route.config.DeliveryProperties;
import com.crossoverjie.cim.route.metric.CimRouteMetrics;
import com.crossoverjie.cim.route.service.DeliveryRetryManager;
import com.crossoverjie.cim.server.api.ServerApi;
import com.crossoverjie.cim.server.api.vo.req.SendMsgReqVO;
import com.crossoverjie.cim.server.api.vo.res.SendMsgResVO;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class OfflineMsgServiceImplTest {

    @Mock
    private OfflineMsgStore offlineMsgStore;
    @Mock
    private ServerApi serverApi;
    @Mock
    private DeliveryRetryManager deliveryRetryManager;
    @Mock
    private CimRouteMetrics cimRouteMetrics;

    private final OfflineMsgServiceImpl service = new OfflineMsgServiceImpl();
    private final DeliveryProperties deliveryProperties = new DeliveryProperties();

    @BeforeEach
    public void setUp() {
        deliveryProperties.setOfflineBatchSize(30);
        ReflectionTestUtils.setField(service, "offlineMsgStore", offlineMsgStore);
        ReflectionTestUtils.setField(service, "serverApi", serverApi);
        ReflectionTestUtils.setField(service, "deliveryRetryManager", deliveryRetryManager);
        ReflectionTestUtils.setField(service, "cimRouteMetrics", cimRouteMetrics);
        ReflectionTestUtils.setField(service, "deliveryProperties", deliveryProperties);
    }

    @Test
    public void fetchPushesOnceThenMarksAndSchedulesEachId() {
        Long receiveUserId = 100L;
        when(offlineMsgStore.fetch(eq(receiveUserId), eq(30)))
                .thenReturn(List.of(msg(3L, "c", receiveUserId), msg(1L, "a", receiveUserId),
                        msg(2L, "b", receiveUserId)))
                .thenReturn(List.of());
        when(serverApi.sendMsg(any(SendMsgReqVO.class), any())).thenReturn(pushedOk());

        service.fetchOfflineMsgs(new CIMServerResVO("127.0.0.1", 11211, 8081), receiveUserId);

        ArgumentCaptor<SendMsgReqVO> captor = ArgumentCaptor.forClass(SendMsgReqVO.class);
        verify(serverApi, times(1)).sendMsg(captor.capture(), eq("http://127.0.0.1:8081"));
        SendMsgReqVO req = captor.getValue();
        Assertions.assertEquals(List.of("a", "b", "c"), req.getBatchMsg());
        Assertions.assertEquals(List.of(1L, 2L, 3L), req.getBatchMessageIds());
        Assertions.assertNull(req.getProperties().get(Constants.MetaKey.MESSAGE_ID));
        Assertions.assertEquals("100", req.getProperties().get(Constants.MetaKey.RECEIVE_USER_ID));

        verify(offlineMsgStore).markSent(receiveUserId, List.of(1L, 2L, 3L));
        verify(deliveryRetryManager).scheduleAfterSent(1L);
        verify(deliveryRetryManager).scheduleAfterSent(2L);
        verify(deliveryRetryManager).scheduleAfterSent(3L);
    }

    @Test
    public void fetchKeepsPendingWhenChannelUnreachable() {
        Long receiveUserId = 100L;
        when(offlineMsgStore.fetch(eq(receiveUserId), anyInt()))
                .thenReturn(List.of(msg(1L, "a", receiveUserId)));
        SendMsgResVO body = new SendMsgResVO();
        body.setPushed(false);
        BaseResponse<SendMsgResVO> response = new BaseResponse<>();
        response.setCode(StatusEnum.OFF_LINE.getCode());
        response.setDataBody(body);
        when(serverApi.sendMsg(any(SendMsgReqVO.class), any())).thenReturn(response);

        service.fetchOfflineMsgs(new CIMServerResVO("127.0.0.1", 11211, 8081), receiveUserId);

        verify(offlineMsgStore, never()).markSent(any(), any());
        verify(deliveryRetryManager, never()).scheduleAfterSent(any());
    }

    @Test
    public void sharedPropertiesDropsFirstMessageId() {
        OfflineMsg first = msg(9L, "x", 1L);
        first.setProperties(Map.of(
                Constants.MetaKey.MESSAGE_ID, "9",
                Constants.MetaKey.SEND_USER_ID, "2",
                Constants.MetaKey.RECEIVE_USER_ID, "1"));
        Map<String, String> properties = OfflineMsgServiceImpl.buildSharedProperties(List.of(first), 1L);
        Assertions.assertNull(properties.get(Constants.MetaKey.MESSAGE_ID));
        Assertions.assertEquals("1", properties.get(Constants.MetaKey.RECEIVE_USER_ID));
        Assertions.assertEquals("2", properties.get(Constants.MetaKey.SEND_USER_ID));
    }

    private static OfflineMsg msg(Long id, String content, Long receiveUserId) {
        return OfflineMsg.builder()
                .messageId(id)
                .content(content)
                .receiveUserId(receiveUserId)
                .properties(Map.of(
                        Constants.MetaKey.SEND_USER_ID, "2",
                        Constants.MetaKey.RECEIVE_USER_ID, String.valueOf(receiveUserId)))
                .build();
    }

    private static BaseResponse<SendMsgResVO> pushedOk() {
        SendMsgResVO body = new SendMsgResVO();
        body.setPushed(true);
        BaseResponse<SendMsgResVO> response = new BaseResponse<>();
        response.setCode(StatusEnum.SUCCESS.getCode());
        response.setDataBody(body);
        return response;
    }
}
