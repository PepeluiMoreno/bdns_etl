import { createRouter, createWebHistory } from 'vue-router'
import { useAuthStore } from '@/stores/auth'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    {
      path: '/login',
      name: 'Login',
      component: () => import('@/views/LoginView.vue'),
      meta: { requiresAuth: false }
    },
    {
      path: '/',
      name: 'Dashboard',
      component: () => import('@/views/DashboardView.vue'),
      meta: { requiresAuth: true }
    },
    {
      path: '/seeding',
      name: 'Seeding',
      component: () => import('@/views/SeedingView.vue'),
      meta: { requiresAuth: true, requiresAdmin: true }
    },
    {
      path: '/sync',
      name: 'Sync',
      component: () => import('@/views/SyncView.vue'),
      meta: { requiresAuth: true, requiresAdmin: true }
    },
    {
      path: '/executions',
      name: 'Executions',
      component: () => import('@/views/ExecutionsView.vue'),
      meta: { requiresAuth: true }
    }
  ]
})

// Navigation guard para autenticación
router.beforeEach((to, from, next) => {
  const authStore = useAuthStore()

  if (to.meta.requiresAuth && !authStore.isAuthenticated) {
    next({ name: 'Login', query: { redirect: to.fullPath } })
  } else if (to.meta.requiresAdmin && authStore.user?.role !== 'admin') {
    next({ name: 'Dashboard' })
  } else if (to.name === 'Login' && authStore.isAuthenticated) {
    next({ name: 'Dashboard' })
  } else {
    next()
  }
})

export default router
