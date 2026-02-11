<template>
  <div class="flex h-screen bg-gray-50 overflow-hidden">
    <!-- Sidebar -->
    <aside
      :class="sidebarOpen ? 'translate-x-0' : '-translate-x-full'"
      class="fixed inset-y-0 left-0 z-50 w-64 bg-gray-900 transition-transform duration-300 ease-in-out lg:translate-x-0 lg:static lg:inset-0"
    >
      <!-- Logo/Brand -->
      <div class="flex items-center justify-between h-16 px-6 bg-gray-800">
        <div class="flex items-center space-x-3">
          <div class="w-8 h-8 bg-blue-600 rounded-lg flex items-center justify-center">
            <ArrowsRightLeftIcon class="w-5 h-5 flex-shrink-0 text-white" />
          </div>
          <div class="flex flex-col">
            <span class="text-xl font-bold text-white leading-tight">BDNS</span>
            <span class="text-sm font-semibold text-gray-400">Panel de ETL v. 0.1.0</span>
          </div>
        </div>
        <button
          @click="sidebarOpen = false"
          class="lg:hidden text-gray-400 hover:text-white"
        >
          <XMarkIcon class="w-6 h-6 flex-shrink-0" />
        </button>
      </div>

      <!-- Navigation -->
      <nav class="mt-6 px-3">
        <div class="space-y-1">
          <!-- Dashboard -->
          <router-link to="/" v-slot="{ isActive }">
            <div
              :class="[
                isActive
                  ? 'bg-gray-800 text-white'
                  : 'text-gray-400 hover:bg-gray-800 hover:text-white',
                'group flex items-center px-3 py-2 text-sm font-medium rounded-lg transition-colors cursor-pointer'
              ]"
            >
              <HomeIcon class="mr-3 h-5 w-5 flex-shrink-0" />
              <span>Panel de Control</span>
            </div>
          </router-link>

          <!-- Procesos -->
          <router-link to="/processes" v-slot="{ isActive }">
            <div
              :class="[
                isActive
                  ? 'bg-gray-800 text-white'
                  : 'text-gray-400 hover:bg-gray-800 hover:text-white',
                'group flex items-center px-3 py-2 text-sm font-medium rounded-lg transition-colors cursor-pointer'
              ]"
            >
              <ClockIcon class="mr-3 h-5 w-5 flex-shrink-0" />
              <span>Procesos</span>
            </div>
          </router-link>
        </div>
      </nav>

      <!-- Panel de estado del sistema -->
      <PanelEstadoSistema />

      <!-- User info (bottom) -->
      <div class="absolute bottom-0 w-64 p-4 bg-gray-800 border-t border-gray-700">
        <div class="flex items-center">
          <div class="flex-shrink-0">
            <div class="w-10 h-10 rounded-full bg-blue-600 flex items-center justify-center">
              <span class="text-sm font-medium text-white">
                {{ authStore.user?.username?.charAt(0).toUpperCase() }}
              </span>
            </div>
          </div>
          <div class="ml-3 flex-1">
            <p class="text-sm font-medium text-white">{{ authStore.user?.username }}</p>
            <p class="text-xs text-gray-400 capitalize">{{ authStore.user?.role }}</p>
          </div>
          <button
            @click="handleLogout"
            class="ml-2 p-2 text-gray-400 hover:text-white rounded-lg hover:bg-gray-700 transition-colors"
            title="Cerrar sesión"
          >
            <ArrowRightStartOnRectangleIcon class="w-5 h-5 flex-shrink-0" />
          </button>
        </div>
      </div>
    </aside>

    <!-- Main Content -->
    <div class="flex-1 flex flex-col overflow-hidden">
      <!-- Top Header -->
      <header class="bg-white border-b border-gray-200 z-10">
        <div class="flex items-center justify-between h-16 px-6">
          <button
            @click="sidebarOpen = true"
            class="lg:hidden text-gray-500 hover:text-gray-700"
          >
            <Bars3Icon class="w-6 h-6 flex-shrink-0" />
          </button>

          <div class="flex-1 lg:ml-0 ml-4">
            <h1 class="text-xl font-semibold text-gray-900">{{ pageTitle }}</h1>
          </div>

          <!-- Header actions -->
          <div class="flex items-center space-x-4">
            <!-- Notifications -->
            <button class="p-2 text-gray-400 hover:text-gray-600 rounded-lg hover:bg-gray-100 transition-colors">
              <BellIcon class="w-6 h-6 flex-shrink-0" />
            </button>

            <!-- Settings -->
            <button class="p-2 text-gray-400 hover:text-gray-600 rounded-lg hover:bg-gray-100 transition-colors">
              <Cog6ToothIcon class="w-6 h-6 flex-shrink-0" />
            </button>
          </div>
        </div>
      </header>

      <!-- Page Content -->
      <main class="flex-1 overflow-y-auto bg-gray-50">
        <div class="p-6">
          <router-view />
        </div>
      </main>
    </div>

    <!-- Mobile sidebar overlay -->
    <div
      v-if="sidebarOpen"
      @click="sidebarOpen = false"
      class="fixed inset-0 bg-gray-900 bg-opacity-50 z-40 lg:hidden"
    ></div>

  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { useAuthStore } from '@/stores/auth'
import { useSystemStore } from '@/stores/system'
import PanelEstadoSistema from '@/components/PanelEstadoSistema.vue'
import {
  HomeIcon,
  ClockIcon,
  ArrowsRightLeftIcon,
  BellIcon,
  Cog6ToothIcon,
  ArrowRightStartOnRectangleIcon,
  Bars3Icon,
  XMarkIcon
} from '@heroicons/vue/24/outline'

const router = useRouter()
const route = useRoute()
const authStore = useAuthStore()
const systemStore = useSystemStore()

const sidebarOpen = ref(false)

let statusInterval = null

// Page title dinámico
const pageTitle = computed(() => {
  const titles = {
    '/': 'Panel de Control',
    '/poblamiento': 'Panel de Control',
    '/seeding': 'Seeding - Carga Inicial',
    '/sync': 'Sincronización',
    '/processes': 'Procesos ETL'
  }
  return titles[route.path] || 'Panel de Control'
})

function handleLogout() {
  authStore.logout()
  router.push('/login')
}

onMounted(() => {
  systemStore.checkSystemStatus()
  statusInterval = setInterval(() => {
    systemStore.checkSystemStatus()
  }, 60000)
})

onUnmounted(() => {
  if (statusInterval) {
    clearInterval(statusInterval)
  }
})
</script>
