import { start } from './index';
import AppNameStrategy from './custom-strategies/app-name-strategy';

start({
    customStrategies: [new AppNameStrategy()],
});
