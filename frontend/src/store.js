import { configureStore } from '@reduxjs/toolkit'
import searchReducer from './features/search/searchSlice'
import entrepriseReducer from './features/entreprise/entrepriseSlice'

export const store = configureStore({
  reducer: {
    search: searchReducer,
    entreprise: entrepriseReducer,
  },
})
