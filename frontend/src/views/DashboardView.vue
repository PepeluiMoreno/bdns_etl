<template>
  <div class="min-h-screen bg-gray-100">
    <!-- Header -->
    <header class="bg-white shadow">
      <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4 flex justify-between items-center">
        <h1 class="text-2xl font-bold text-gray-900">ETL Admin Dashboard</h1>
        <div class="flex items-center space-x-4">
          <span class="text-sm text-gray-600">{{ authStore.user?.username }}</span>
          <span class="px-2 py-1 text-xs rounded-full bg-blue-100 text-blue-800">
            {{ authStore.user?.role }}
          </span>
          <button
            @click="handleLogout"
            class="px-4 py-2 text-sm font-medium text-white bg-red-600 hover:bg-red-700 rounded-md"
          >
            Cerrar sesión
          </button>
        </div>
      </div>
    </header>

    <!-- Navigation -->
    <nav class="bg-white shadow-sm">
      <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div class="flex space-x-8">
          <router-link
            to="/"
            class="px-3 py-4 text-sm font-medium text-gray-900 border-b-2 border-blue-500"
          >
            Dashboard
          </router-link>
          <router-link
            v-if="authStore.isAdmin"
            to="/seeding"
            class="px-3 py-4 text-sm font-medium text-gray-600 hover:text-gray-900"
          >
            Seeding
          </router-link>
          <router-link
            v-if="authStore.isAdmin"
            to="/sync"
            class="px-3 py-4 text-sm font-medium text-gray-600 hover:text-gray-900"
          >
            Sincronización
          </router-link>
          <router-link
            to="/executions"
            class="px-3 py-4 text-sm font-medium text-gray-600 hover:text-gray-900"
          >
            Ejecuciones
          </router-link>
        </div>
      </div>
    </nav>

    <!-- Main Content -->
    <main class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <div class="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
        <!-- Stats Cards -->
        <div class="bg-white rounded-lg shadow p-6">
          <div class="text-sm font-medium text-gray-600">Total Ejecuciones</div>
          <div class="mt-2 text-3xl font-bold text-gray-900">{{ stats.total_executions?.total || 0 }}</div>
        </div>

        <div class="bg-white rounded-lg shadow p-6">
          <div class="text-sm font-medium text-gray-600">Procesos Activos</div>
          <div class="mt-2 text-3xl font-bold text-green-600">{{ stats.active_processes || 0 }}</div>
        </div>

        <div class="bg-white rounded-lg shadow p-6">
          <div class="text-sm font-medium text-gray-600">Último Seeding</div>
          <div class="mt-2 text-sm text-gray-900">
            {{ formatDate(stats.last_successful?.seeding) }}
          </div>
        </div>
      </div>

      <!-- Recent Executions -->
      <div class="bg-white rounded-lg shadow">
        <div class="px-6 py-4 border-b border-gray-200">
          <h2 class="text-lg font-medium text-gray-900">Ejecuciones Recientes</h2>
        </div>
        <div class="p-6">
          <div v-if="loading" class="text-center py-8 text-gray-500">
            Cargando...
          </div>
          <div v-else-if="recentExecutions.length === 0" class="text-center py-8 text-gray-500">
            No hay ejecuciones registradas
          </div>
          <table v-else class="min-w-full divide-y divide-gray-200">
            <thead>
              <tr>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Tipo
                </th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Entidad
                </th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Estado
                </th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Fecha
                </th>
              </tr>
            </thead>
            <tbody class="bg-white divide-y divide-gray-200">
              <tr v-for="execution in recentExecutions" :key="execution.execution_id">
                <td class="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                  {{ execution.process_type }}
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                  {{ execution.entity }}
                </td>
                <td class="px-6 py-4 whitespace-nowrap">
                  <span :class="getStatusClass(execution.status)" class="px-2 py-1 text-xs rounded-full">
                    {{ execution.status }}
                  </span>
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                  {{ formatDate(execution.started_at) }}
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </main>
  </div>
</template>

<script setup>
import { ref, onMounted, onUnmounted } from 'vue'
import { useRouter } from 'vue-router'
import { useAuthStore } from '@/stores/auth'
import axios from 'axios'

const router = useRouter()
const authStore = useAuthStore()

const stats = ref({})
const recentExecutions = ref([])
const loading = ref(true)
let wsConnection = null

onMounted(() => {
  loadData()
  connectWebSocket()
})

onUnmounted(() => {
  if (wsConnection) {
    wsConnection.close()
  }
})

async function loadData() {
  try {
    const [statsRes, executionsRes] = await Promise.all([
      axios.get('/api/etl/statistics'),
      axios.get('/api/etl/executions?limit=10')
    ])

    stats.value = statsRes.data
    recentExecutions.value = executionsRes.data
  } catch (error) {
    console.error('Error loading data:', error)
  } finally {
    loading.value = false
  }
}

function connectWebSocket() {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  const wsUrl = `${protocol}//localhost:8001/api/etl/ws`

  wsConnection = new WebSocket(wsUrl)

  wsConnection.onmessage = (event) => {
    const data = JSON.parse(event.data)
    if (data.type === 'stats_update') {
      stats.value = data.data.statistics
      recentExecutions.value = data.data.recent_executions
    }
  }

  wsConnection.onerror = (error) => {
    console.error('WebSocket error:', error)
  }

  wsConnection.onclose = () => {
    // Reconectar después de 5 segundos
    setTimeout(connectWebSocket, 5000)
  }
}

function handleLogout() {
  authStore.logout()
  router.push('/login')
}

function formatDate(dateString) {
  if (!dateString) return 'N/A'
  return new Date(dateString).toLocaleString('es-ES')
}

function getStatusClass(status) {
  const classes = {
    completed: 'bg-green-100 text-green-800',
    running: 'bg-blue-100 text-blue-800',
    failed: 'bg-red-100 text-red-800',
    cancelled: 'bg-gray-100 text-gray-800'
  }
  return classes[status] || 'bg-gray-100 text-gray-800'
}
</script>
