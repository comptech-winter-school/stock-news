import streamlit as st


def main():
    st.title('Hi!')

    if st.button('Click me'):
        st.write('Works!')


if __name__ == '__main__':
    main()
