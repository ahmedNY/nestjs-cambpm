import {
  Server,
  CustomTransportStrategy,
  MessageHandler,
} from '@nestjs/microservices';
import {
  Inject,
  Injectable,
  Logger
} from '@nestjs/common';
import { Client } from 'camunda-external-task-client-js';

@Injectable()
export class ExternalTaskConnector extends Server
  implements CustomTransportStrategy {
  constructor(@Inject('CONNECTION_PROVIDER') private readonly client: Client) {
    super();
  }

  public async listen(callback: () => void) {
    this.init();
    callback();
  }

  public close() {
    this.client.stop();
    Logger.log('External Task Client stopped', 'ExternalTaskConnector')
  }

  protected init(): void {
    this.client.start();

    Logger.log('External Task Client started', 'CamundaTaskConnector');

    const handlers = this.getHandlers();
    /* istanbul ignore next */
    handlers.forEach((messageHandler: MessageHandler, key: string) => {
      const { topic, options } = JSON.parse(key);

      this.client.subscribe(topic, options, async ({ task, taskService }) => {
        await messageHandler(task, taskService);
      });
    });
  }
}
