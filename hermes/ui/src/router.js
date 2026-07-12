import { createRouter, createWebHistory } from 'vue-router'
import Dashboard from './views/Dashboard.vue'
import Analytics from './views/Analytics.vue'
import ChartVision from './views/ChartVision.vue'
import StrategyDocs from './views/StrategyDocs.vue'
import Settings from './views/Settings.vue'

// Route names double as the visible page titles — keep them in sync with the
// sidebar labels in App.vue.
const routes = [
  {
    path: '/',
    name: 'Dashboard',
    component: Dashboard
  },
  {
    path: '/analytics/:tab?',
    name: 'Analytics',
    component: Analytics
  },
  {
    path: '/charts',
    name: 'Markets',
    component: ChartVision
  },
  {
    path: '/docs',
    name: 'Docs',
    component: StrategyDocs
  },
  {
    path: '/settings',
    name: 'Settings',
    component: Settings
  }
]

const router = createRouter({
  history: createWebHistory(),
  routes
})

export default router
