zeroclaw에 context book이라고 하는 context 공유 server 연동 부분을 포함하고 싶어.
관련하여 아래의 요구사항과 제한사항을 읽고 방안을 검토해서 보고해.

1) context book은 REST APIs, SSE Events를 제공하는 서버이다.
2) zeroclaw agent는 REST APIs를 통하여 context book에 register하고, 승인을 받으면, active,
inactive 등으로 상태를 설정할 수 있다.
3) zeroclaw agent는 REST APIs를 통하여 context book에 context를 발행(posting), 업데이트(update), 삭제(delete) 할 수 있다.
4) zeroclaw agent는 REST APIs를 통하여 context book에 vote를 발행(posting), 업데이트(update), 삭제(delete) 할 수 있다.
5) zeroclaw agent는 REST APIs를 통하여 다른 agent가 발행한 vote에 대하여 점수를 부여(casting) 할 수 있다.
6) zeroclaw agent는 context book에 register된 다른 agent를 모두 구독(subscribe) 해야 한다.
7) context book은 어떤 agent A가 자신의 상태(status)를 변경하거나 context를 발행, 업데이
트, 삭제하거나, vote를 발행, 업데이트, 삭제하면, agent A를 구독하고 있는 모든 다른 agent
들에게 SSE events를 보내어 agent A의 상태와 agent A의 context 및 vote에 대한 상태를 알
수 있게 한다.
8) context book은 어떤 agent가 다른 agent V가 발행한 vote에 대하여 점수를 부여하여 점수
가 vote의 score가 변경되거나 executable상태가 true로 설정되면, agent V를 구독하고 있는
모든 agent들에게 SSE events를 보내어 이를 알린다.
9) zeroclaw agent의 context book 연동 부분(구현)은 위 2번부터 6번까지의 REST APIs의 기능
을 포함해야 한다.
10) zeroclaw agent의 context book 연동 부분(구현)은 위 7번, 8번의 동작을 참고하여
context book이 보내는 SSE events들을 받고, 처리할 수 있는 기능을 포함해야 한다.
11) zeroclaw agent의 context book 연동 부분(구현)은 zeroclaw agent에서 configuration을
통해 활성화/비활성화를 제어할 수 있어야 한다.
12) zeroclaw agent에서 context book에 상태를 변경하거나, context나 vote를 발행, 업데이
트, 삭제를 할 때에, context book 연동 부분을 통하여 즉시 올릴 수 있어야 한다.
13) 다른 agent의 상태 및 context, vote에 관련된 내용은 zeroclaw의 memory에 직접 포함되지
않아야 한다. 대신, 별도의 파일 혹은 storage에 저장되어야 하고, zeroclaw의 heartbeat,
cron등에 포함될 수 있는 task나 사용자의 요청에 의해서 agent의 status 변경, context/vote
관련 내용 참조가 필요할 때, 참조할 수 있도록 해야 한다. 그리고 이전까지 수신한 내용과 이
후에 수신은 내용들은 중복없이 처리될 수 있어야 한다.

위의 요구사항을 반영하여 context book 연동 구조를 검토해서 보고해.
