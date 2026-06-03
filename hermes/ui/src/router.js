import { createRouter, createWebHistory } from 'vue-router'
import Dashboard from './views/Dashboard.vue'
import Analytics from './views/Analytics.vue'
import ChartVision from './views/ChartVision.vue'

const routes = [
  {
    path: '/',
    name: 'Dashboard',
    component: Dashboard
  },
  {
    path: '/analytics',
    name: 'Analytics',
    component: Analytics
  },
  {
    path: '/charts',
    name: 'ChartVision',
    component: ChartVision
  }
]

const router = createRouter({
  history: createWebHistory(),
  routes
})

export default router
