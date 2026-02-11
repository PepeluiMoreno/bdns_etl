import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import axios from 'axios'

const API_URL = `${import.meta.env.VITE_API_ENDPOINT}/auth`

export const useAuthStore = defineStore('auth', () => {
  // State
  const accessToken = ref(localStorage.getItem('access_token') || null)
  const refreshToken = ref(localStorage.getItem('refresh_token') || null)
  const user = ref(JSON.parse(localStorage.getItem('user') || 'null'))

  // Getters
  const isAuthenticated = computed(() => !!accessToken.value)
  const isAdmin = computed(() => user.value?.role === 'admin')

  // Actions
  async function login(username, password) {
    try {
      const response = await axios.post(`${API_URL}/login`, {
        username,
        password
      })

      const { access_token, refresh_token } = response.data

      // Guardar tokens
      accessToken.value = access_token
      refreshToken.value = refresh_token
      localStorage.setItem('access_token', access_token)
      localStorage.setItem('refresh_token', refresh_token)

      // Obtener datos de usuario
      await fetchUser()

      return true
    } catch (error) {
      console.error('Login error:', error)
      throw error
    }
  }

  async function fetchUser() {
    try {
      const response = await axios.get(`${API_URL}/me`, {
        headers: {
          Authorization: `Bearer ${accessToken.value}`
        }
      })

      user.value = response.data
      localStorage.setItem('user', JSON.stringify(response.data))
    } catch (error) {
      console.error('Fetch user error:', error)
      logout()
    }
  }

  async function refreshAccessToken() {
    try {
      const response = await axios.post(`${API_URL}/refresh`, {
        refresh_token: refreshToken.value
      })

      const { access_token } = response.data
      accessToken.value = access_token
      localStorage.setItem('access_token', access_token)

      return true
    } catch (error) {
      console.error('Refresh token error:', error)
      logout()
      return false
    }
  }

  function logout() {
    accessToken.value = null
    refreshToken.value = null
    user.value = null
    localStorage.removeItem('access_token')
    localStorage.removeItem('refresh_token')
    localStorage.removeItem('user')
  }

  // Configurar interceptor de Axios para añadir token automáticamente
  axios.interceptors.request.use(
    (config) => {
      if (accessToken.value && !config.headers.Authorization) {
        config.headers.Authorization = `Bearer ${accessToken.value}`
      }
      return config
    },
    (error) => Promise.reject(error)
  )

  // Interceptor para manejar 401 y refrescar token
  axios.interceptors.response.use(
    (response) => response,
    async (error) => {
      const originalRequest = error.config

      if (error.response?.status === 401 && !originalRequest._retry) {
        originalRequest._retry = true

        const success = await refreshAccessToken()
        if (success) {
          originalRequest.headers.Authorization = `Bearer ${accessToken.value}`
          return axios(originalRequest)
        }
      }

      return Promise.reject(error)
    }
  )

  return {
    accessToken,
    refreshToken,
    user,
    isAuthenticated,
    isAdmin,
    login,
    logout,
    fetchUser,
    refreshAccessToken
  }
})
